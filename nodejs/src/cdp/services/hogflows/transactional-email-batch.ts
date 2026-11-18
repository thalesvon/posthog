import { Counter, Histogram } from 'prom-client'
import { z } from 'zod'

import { logger } from '~/common/utils/logger'

import { Settlement } from '../../intake/intake-producer'
import { Semaphore } from '../../utils/sempahore'
import { BatchSendSchema, TransactionalEmailRequestSchema } from './transactional-email'

export const TEMPLATE_BATCH_TOKEN_CLAIMS = { kind: 'template_batch' }
export const CODE_SEND_BATCH_TOKEN_CLAIMS = { kind: 'code_send_batch' }

export const BATCH_MAX_RECIPIENTS = 100
export const BATCH_FAN_OUT = 25
// Below Django's 30 s forward timeout, so a stuck recipient never costs the caller the other recipients' answers.
export const BATCH_RESPONSE_DEADLINE_MS = 20_000

const Variables = z.record(z.string(), z.any())
const IdempotencyKey = z.string().min(1).max(255)

// The address and the person are checked per recipient, so one bad recipient refuses only its own slot.
const BatchRecipient = z.object({ to: z.string(), distinct_id: z.string(), idempotency_key: IdempotencyKey })

const hasUniqueKeys = (recipients: { idempotency_key: string }[]): boolean =>
    new Set(recipients.map((recipient) => recipient.idempotency_key)).size === recipients.length
const UNIQUE_KEYS = { message: 'idempotency_key must be unique within a batch' }

// What Django sends to run one email for many recipients. Strict, so a field a batch never takes (a send time, copies,
// attachments) refuses the whole call instead of reaching a recipient's run.
export const TemplateBatchRequestSchema = z
    .object({
        variables: Variables.default({}),
        recipients: z
            .array(BatchRecipient.extend({ variables: Variables.optional() }).strict())
            .min(1)
            .max(BATCH_MAX_RECIPIENTS)
            .refine(hasUniqueKeys, UNIQUE_KEYS),
    })
    .strict()

export const CodeSendBatchRequestSchema = z
    .object({
        variables: Variables.default({}),
        send: BatchSendSchema,
        recipients: z
            .array(BatchRecipient.strict())
            .min(1)
            .max(BATCH_MAX_RECIPIENTS)
            .refine(hasUniqueKeys, UNIQUE_KEYS),
    })
    .strict()

export type TransactionalEmailBatchRequest = {
    variables: Record<string, any>
    send?: z.infer<typeof BatchSendSchema>
    recipients: { to: string; distinct_id: string; idempotency_key: string; variables?: Record<string, any> }[]
}
export type BatchRecipient = TransactionalEmailBatchRequest['recipients'][number]

// One recipient's send, checked as a single send is.
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

export type SlotReply = { outcome: string; status: number; body: Record<string, unknown> }

export type BatchSlot = {
    settlement: Settlement
    // Called before the step that can queue the email, so a later failure answers "may still go out".
    mayCommit: () => void
}

export const NOT_ATTEMPTED: SlotReply = { outcome: 'not_attempted', status: 503, body: { error: 'not_attempted' } }
const unconfirmed = (outcome: string): SlotReply => ({
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
    help: 'Batch email requests from Django, by outcome: answered (with a result per recipient), over_capacity (the pod already ran its share of batches), or a whole-call refusal before any recipient ran. Recipients count in cdp_transactional_email_requests_total with route batch.',
    labelNames: ['outcome'],
})

export const transactionalEmailBatchDurationMs = new Histogram({
    name: 'cdp_transactional_email_batch_duration_ms',
    help: 'Milliseconds from the arrival of an admitted batch email request to its answer.',
    buckets: [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 15_000, 20_000, 30_000],
})

// The pod's share of batch work: a request cap that bounds parsed bodies in memory, and a recipient cap across
// requests that bounds the load on the claim store, the queue and the intake topics.
export class BatchCapacity {
    readonly slots: Semaphore
    private admitted = 0

    constructor(
        private readonly maxRequests: number,
        maxSlots: number
    ) {
        this.slots = new Semaphore(maxSlots)
    }

    // Null when the pod is at its request cap. The caller releases once every recipient has settled.
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

const whenAborted = (signal: AbortSignal): Promise<void> =>
    new Promise((resolve) =>
        signal.aborted ? resolve() : signal.addEventListener('abort', () => resolve(), { once: true })
    )

// Runs each recipient `fanOut` at a time, each within a slot permit, and answers by the deadline: a recipient still
// running answers "may still go out", and one not started answers not_attempted. A recipient that runs past the
// deadline keeps running, and holds its permit until it and its publishes settle; `settled` resolves then.
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
        const publishes: Promise<unknown>[] = []
        let mayHaveCommitted = false
        try {
            replies[i] = await runSlot(items[i], {
                settlement: { track: (publish) => publishes.push(publish) },
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
        await Promise.allSettled(publishes)
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
