import { EventEmitter } from 'events'
import { Counter, Histogram } from 'prom-client'
import { Readable } from 'stream'
import { z } from 'zod'

import { parseJSON } from '~/common/utils/json-parse'

import { deadlineError } from '../../utils/deadline'
import { BatchSendSchema, TransactionalEmailOutcome, TransactionalEmailRequestSchema } from './transactional-email'

export const TEMPLATE_BATCH_TOKEN_CLAIMS = { kind: 'template_batch' }
export const CODE_SEND_BATCH_TOKEN_CLAIMS = { kind: 'code_send_batch' }

export const BATCH_MAX_RECIPIENTS = 100

// Bounds the body read and the shared lookups. Django drops a forwarded call after 30 s, so a batch that answers later
// returns none of its results. The cap leaves 10 s for the checks, the sandbox read, the broker's acks and the trip back.
const BATCH_DEADLINE_MAX_MS = 20_000

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

// The address and the person are checked per recipient, so one bad recipient refuses only its own result.
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

// The same request a single send to this recipient makes, so a single send and a batch recipient replay each other.
export const slotRequestOf = (batch: TransactionalEmailBatchRequest, recipient: BatchRecipient): unknown => ({
    to: recipient.to,
    distinct_id: recipient.distinct_id,
    idempotency_key: recipient.idempotency_key,
    variables: { ...batch.variables, ...recipient.variables },
    ...(batch.send && { send: batch.send }),
})

export type SlotReply = { outcome: TransactionalEmailOutcome; status: number; body: Record<string, unknown> }

export const transactionalEmailBatchRequestsTotal = new Counter({
    name: 'cdp_transactional_email_batch_requests_total',
    help: 'Batch email requests from Django, by outcome: answered (with a result per recipient), over_capacity (the pod already ran its share of batches), batch_unavailable (the team is not on async intake), abandoned (the caller left before its body arrived), or another whole-call refusal, which produces nothing. Recipients count in cdp_transactional_email_requests_total with route batch.',
    labelNames: ['outcome'],
})

export const transactionalEmailBatchDurationMs = new Histogram({
    name: 'cdp_transactional_email_batch_duration_ms',
    help: 'Milliseconds from the arrival of an admitted batch email request to its answer.',
    buckets: [10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 15_000, 20_000, 30_000],
})

export class BatchCapacity {
    private admitted = 0

    constructor(private readonly maxRequests: number) {}

    admit(): (() => void) | null {
        if (this.admitted >= this.maxRequests) {
            return null
        }
        this.admitted++
        return () => {
            this.admitted--
        }
    }
}

export const BATCH_BODY_LIMIT_BYTES = 20 * 1024 * 1024

export type BodyRead =
    | { read: 'parsed'; body: unknown }
    | { read: 'too_large' | 'invalid_request' | 'abandoned' | 'deadline_exceeded' }

// The route reads the body itself, not through ultimate-express's JSON parser, because nothing can stop that parser
// once it has started: it would keep buffering and parsing after the deadline freed the batch's admission. The server
// delivers exactly the declared length, so the declared length bounds the read.
export const readBatchBody = (
    body: Readable,
    { length, connection, deadline }: { length: number; connection: EventEmitter | null; deadline: AbortSignal }
): Promise<BodyRead> =>
    new Promise((resolve) => {
        if (length > BATCH_BODY_LIMIT_BYTES) {
            return resolve({ read: 'too_large' })
        }
        if (deadline.aborted) {
            return resolve({ read: 'deadline_exceeded' })
        }
        const chunks: Buffer[] = []
        const end = (read: BodyRead): void => {
            body.off('data', received)
            body.off('end', ended)
            connection?.off('close', abandoned)
            deadline.removeEventListener('abort', expired)
            resolve(read)
        }
        const received = (chunk: Buffer): void => {
            chunks.push(chunk)
        }
        // The abort timer can run after the last chunk even when the deadline has passed, so the read checks the clock
        // before it parses and again once the parse is done.
        const ended = (): void => {
            if (deadlineError(deadline) !== undefined) {
                return end({ read: 'deadline_exceeded' })
            }
            let read: BodyRead
            try {
                read = { read: 'parsed', body: parseJSON(Buffer.concat(chunks).toString('utf8')) }
            } catch {
                read = { read: 'invalid_request' }
            }
            end(deadlineError(deadline) !== undefined ? { read: 'deadline_exceeded' } : read)
        }
        const abandoned = (): void => end({ read: 'abandoned' })
        const expired = (): void => end({ read: 'deadline_exceeded' })
        connection?.once('close', abandoned)
        deadline.addEventListener('abort', expired, { once: true })
        body.on('data', received)
        body.on('end', ended)
    })

// The work keeps running past the deadline, so its later failure must not go unhandled.
export const withinDeadline = <T>(work: Promise<T>, deadline: AbortSignal): Promise<T | 'deadline_exceeded'> => {
    work.catch(() => undefined)
    if (deadline.aborted) {
        return Promise.resolve('deadline_exceeded')
    }
    let expired: () => void = () => {}
    const aborted = new Promise<'deadline_exceeded'>((resolve) => {
        expired = () => resolve('deadline_exceeded')
        deadline.addEventListener('abort', expired, { once: true })
    })
    return Promise.race([work, aborted]).finally(() => deadline.removeEventListener('abort', expired))
}
