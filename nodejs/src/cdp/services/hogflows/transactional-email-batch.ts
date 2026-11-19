import { EventEmitter } from 'events'
import { Counter, Histogram } from 'prom-client'
import { z } from 'zod'

import { logger } from '~/common/utils/logger'

import { PublishSettlement, newPublishSettlement, publishesSettled } from '../../intake/intake-producer'
import { Semaphore } from '../../utils/sempahore'
import { BatchSendSchema, TransactionalEmailOutcome, TransactionalEmailRequestSchema } from './transactional-email'

export const TEMPLATE_BATCH_TOKEN_CLAIMS = { kind: 'template_batch' }
export const CODE_SEND_BATCH_TOKEN_CLAIMS = { kind: 'code_send_batch' }

export const BATCH_MAX_RECIPIENTS = 100
export const BATCH_FAN_OUT = 25

// Django drops a forwarded call after 30 s, so a batch that answers later returns none of its results. The cap leaves
// 5 s for the answer to travel back and for Django to read it.
const BATCH_DEADLINE_MAX_MS = 25_000

export const batchDeadlineMs = (setting: number): number => {
    if (!Number.isInteger(setting) || setting <= 0 || setting > BATCH_DEADLINE_MAX_MS) {
        throw new Error(
            `CDP_TRANSACTIONAL_EMAIL_BATCH_DEADLINE_MS must be a whole number of milliseconds from 1 to ${BATCH_DEADLINE_MAX_MS}, got ${setting}`
        )
    }
    return setting
}

const Variables = z.record(z.string(), z.any())
const IdempotencyKey = z.string().min(1).max(255)

// The address and the person are checked per recipient, so one bad recipient refuses only its own slot.
const BatchRecipient = z.object({ to: z.unknown(), distinct_id: z.unknown(), idempotency_key: IdempotencyKey })

const hasUniqueKeys = (recipients: { idempotency_key: string }[]): boolean =>
    new Set(recipients.map((recipient) => recipient.idempotency_key)).size === recipients.length

// Zod reads every entry of an array before it checks the array's length. The length is checked on the raw value
// first, so an oversized list costs one issue and no pass over its entries.
const recipientsOf = <T extends { idempotency_key: string }>(recipient: z.ZodType<T>) =>
    z
        .unknown()
        .check((ctx) => {
            if (Array.isArray(ctx.value) && ctx.value.length > BATCH_MAX_RECIPIENTS) {
                ctx.issues.push({
                    code: 'too_big',
                    origin: 'array',
                    maximum: BATCH_MAX_RECIPIENTS,
                    inclusive: true,
                    input: ctx.value,
                })
            }
        })
        .pipe(
            z
                .array(recipient)
                .min(1)
                .refine(hasUniqueKeys, { message: 'idempotency_key must be unique within a batch' })
        )

// What Django sends to run one email for many recipients. Strict, so a field a batch never takes (a send time, copies,
// attachments) refuses the whole call instead of reaching a recipient's run.
export const TemplateBatchRequestSchema = z
    .object({
        variables: Variables.default({}),
        recipients: recipientsOf(BatchRecipient.extend({ variables: Variables.optional() }).strict()),
    })
    .strict()

export const CodeSendBatchRequestSchema = z
    .object({
        variables: Variables.default({}),
        send: BatchSendSchema,
        recipients: recipientsOf(BatchRecipient.strict()),
    })
    .strict()

export type TransactionalEmailBatchRequest = {
    variables: Record<string, any>
    send?: z.infer<typeof BatchSendSchema>
    recipients: { to?: unknown; distinct_id?: unknown; idempotency_key: string; variables?: Record<string, any> }[]
}
export type BatchRecipient = TransactionalEmailBatchRequest['recipients'][number]

export const BatchSlotRequestSchema = TransactionalEmailRequestSchema.omit({ scheduled_at: true }).extend({
    idempotency_key: IdempotencyKey,
    send: BatchSendSchema.optional(),
})

// The same request a single send to this recipient makes, so a single send and a batch slot replay each other.
export const slotRequestOf = (batch: TransactionalEmailBatchRequest, recipient: BatchRecipient): unknown => ({
    to: recipient.to,
    distinct_id: recipient.distinct_id,
    idempotency_key: recipient.idempotency_key,
    variables: { ...batch.variables, ...recipient.variables },
    ...(batch.send && { send: batch.send }),
})

export type SlotOutcome = TransactionalEmailOutcome | 'not_sendable' | 'deadline_running' | 'not_attempted'
export type SlotReply = { outcome: SlotOutcome; status: number; body: Record<string, unknown> }

export type BatchSlot = {
    settlement: PublishSettlement
    // Called before the step that can queue the email, so a later failure answers "may still go out".
    mayCommit: () => void
}

export const NOT_ATTEMPTED: SlotReply = { outcome: 'not_attempted', status: 503, body: { error: 'not_attempted' } }
const unconfirmed = (outcome: SlotOutcome): SlotReply => ({
    outcome,
    status: 503,
    body: { error: 'delivery_unavailable', may_still_go_out: true },
})
const FAILED_BEFORE_COMMIT: SlotReply = {
    outcome: 'error',
    status: 503,
    body: { error: 'delivery_unavailable', may_still_go_out: false },
}

export const transactionalEmailBatchRequestsTotal = new Counter({
    name: 'cdp_transactional_email_batch_requests_total',
    help: 'Batch email requests from Django, by outcome: answered (with a result per recipient), over_capacity (the pod already ran its share of batches), abandoned (the caller left before its body arrived), or a whole-call refusal before any recipient ran. Recipients count in cdp_transactional_email_requests_total with route batch.',
    labelNames: ['outcome'],
})

export const transactionalEmailBatchDurationMs = new Histogram({
    name: 'cdp_transactional_email_batch_duration_ms',
    help: 'Milliseconds from the arrival of an admitted batch email request to its answer.',
    buckets: [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 15_000, 20_000, 30_000],
})

export class BatchCapacity {
    readonly slots: Semaphore
    private admitted = 0

    constructor(
        private readonly maxRequests: number,
        maxSlots: number
    ) {
        this.slots = new Semaphore(maxSlots)
    }

    admit(): (() => void) | null {
        if (this.admitted >= this.maxRequests) {
            return null
        }
        this.admitted++
        let released = false
        return () => {
            if (!released) {
                released = true
                this.admitted--
            }
        }
    }
}

export type BodyRead = 'parsed' | 'too_large' | 'invalid_request' | 'abandoned' | 'deadline_exceeded'

// The parser calls back once per chunk past its size limit and again at the end of the body, and never when the
// caller leaves, so the first of these ends the read and frees the request's admission from a late callback.
export const readBatchBody = (
    parse: (done: (error?: unknown) => void) => void,
    { connection, deadline }: { connection: EventEmitter | null; deadline: AbortSignal }
): Promise<BodyRead> =>
    new Promise((resolve) => {
        const end = (read: BodyRead): void => {
            connection?.off('close', abandoned)
            deadline.removeEventListener('abort', expired)
            resolve(read)
        }
        const abandoned = (): void => end('abandoned')
        const expired = (): void => end('deadline_exceeded')
        if (deadline.aborted) {
            return end('deadline_exceeded')
        }
        connection?.once('close', abandoned)
        deadline.addEventListener('abort', expired, { once: true })
        parse((error) =>
            end(
                !error
                    ? 'parsed'
                    : error instanceof Error && error.message === 'Request entity too large'
                      ? 'too_large'
                      : 'invalid_request'
            )
        )
    })

const whenAborted = (signal: AbortSignal): Promise<void> =>
    new Promise((resolve) =>
        signal.aborted ? resolve() : signal.addEventListener('abort', () => resolve(), { once: true })
    )

// Answers by the deadline: a recipient still running "may still go out", and one not started is not_attempted. A
// recipient past the deadline keeps running and holds its permit until it and its publishes settle, as `settled` does.
export const runBatchSlots = async <T>(
    items: readonly T[],
    runSlot: (item: T, slot: BatchSlot) => Promise<SlotReply>,
    { slots, fanOut, deadline }: { slots: Semaphore; fanOut: number; deadline: AbortSignal }
): Promise<{ replies: SlotReply[]; settled: Promise<void> }> => {
    const replies: (SlotReply | undefined)[] = items.map(() => undefined)
    const started = items.map(() => false)
    let next = 0
    let answered = 0
    let allAnswered: () => void = () => {}
    const everyRecipientAnswered = new Promise<void>((resolve) => (allAnswered = resolve))

    const runOne = async (i: number): Promise<void> => {
        started[i] = true
        const settlement = newPublishSettlement()
        let mayHaveCommitted = false
        try {
            replies[i] = await runSlot(items[i], {
                settlement,
                mayCommit: () => (mayHaveCommitted = true),
            })
        } catch (error) {
            logger.error('Error sending a batch email recipient', {
                error: error instanceof Error ? error.message : String(error),
            })
            replies[i] = mayHaveCommitted ? unconfirmed('error') : FAILED_BEFORE_COMMIT
        }
        if (++answered === items.length) {
            allAnswered()
        }
        await publishesSettled(settlement)
    }
    const worker = async (): Promise<void> => {
        while (next < items.length && !deadline.aborted) {
            const i = next++
            try {
                await slots.run(() => runOne(i), deadline)
            } catch {
                // The deadline passed while this recipient waited for a permit, so it never started.
            }
        }
    }

    const settled = Promise.all(Array.from({ length: Math.min(fanOut, items.length) }, worker)).then(() => undefined)
    await Promise.race([everyRecipientAnswered, settled, whenAborted(deadline)])
    return {
        replies: items.map((_, i) => replies[i] ?? (started[i] ? unconfirmed('deadline_running') : NOT_ATTEMPTED)),
        settled,
    }
}
