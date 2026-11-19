import { PassThrough } from 'stream'

import { deadlineAfter } from '../../utils/deadline'
import {
    BatchCapacity,
    CodeSendBatchRequestSchema,
    TemplateBatchRequestSchema,
    batchDeadlineMs,
    readBatchBody,
} from './transactional-email-batch'

describe('transactional email batch', () => {
    describe('BatchCapacity', () => {
        it('refuses a request over the cap until an admitted one releases', () => {
            const capacity = new BatchCapacity(1)
            const release = capacity.admit()

            expect(release).not.toBeNull()
            expect(capacity.admit()).toBeNull()

            release!()

            expect(capacity.admit()).not.toBeNull()
            expect(capacity.admit()).toBeNull()
        })
    })

    describe('readBatchBody', () => {
        afterEach(() => jest.restoreAllMocks())

        // The clock passes the deadline while its abort timer has not run yet, which an event-loop stall causes in a pod.
        it.each([
            {
                what: 'a body that ends after its deadline passed',
                text: '{"recipients": []}',
                stage: (passDeadline: () => void) => passDeadline(),
            },
            {
                what: 'a malformed body whose parse runs past its deadline',
                text: '{"recipients": [',
                stage: (passDeadline: () => void) =>
                    jest.spyOn(JSON, 'parse').mockImplementation(() => {
                        passDeadline()
                        throw new SyntaxError('Unexpected end of JSON input')
                    }),
            },
        ])('answers $what as deadline_exceeded', async ({ text, stage }) => {
            const deadline = deadlineAfter(60_000)
            let now = performance.now()
            jest.spyOn(performance, 'now').mockImplementation(() => now)
            const body = new PassThrough()
            const read = readBatchBody(body, { length: Buffer.byteLength(text), connection: null, deadline })
            body.write(text.slice(0, 1))
            stage(() => (now += 60_000))

            body.end(text.slice(1))

            expect(await read).toEqual({ read: 'deadline_exceeded' })
        })
    })

    describe('batchDeadlineMs', () => {
        it.each([-1, 0, 2.5, 20_001, 29_999])('refuses a deadline of %s ms', (setting) => {
            expect(() => batchDeadlineMs(setting)).toThrow('CDP_TRANSACTIONAL_EMAIL_BATCH_DEADLINE_MS')
        })

        it('keeps a deadline that leaves Django time to read the answer', () => {
            expect(batchDeadlineMs(20_000)).toBe(20_000)
        })
    })

    describe('TemplateBatchRequestSchema', () => {
        it.each([
            [100, true],
            [101, false],
        ])('takes a list of %i recipients: %s', (count, accepted) => {
            const recipients = Array.from({ length: count }, (_, i) => ({
                to: `person-${i}@example.com`,
                distinct_id: `person-${i}`,
                idempotency_key: `key-${i}`,
            }))

            expect(TemplateBatchRequestSchema.safeParse({ recipients }).success).toBe(accepted)
        })

        it('refuses a recipient list over the cap with one issue for the whole list', () => {
            const parsed = TemplateBatchRequestSchema.safeParse({
                recipients: Array.from({ length: 1000 }, () => ({})),
            })

            expect(parsed.error?.issues).toEqual([expect.objectContaining({ code: 'too_big', path: ['recipients'] })])
        })
    })

    describe('whole-batch refusals', () => {
        const ada = { to: 'ada@example.com', distinct_id: 'person-ada', idempotency_key: 'key-ada' }
        const bo = { to: 'bo@example.com', distinct_id: 'person-bo', idempotency_key: 'key-bo' }
        const send = { content: { subject: 'Hello', text: 'Hi' }, tracking: { captureEngagement: false } }

        it.each([
            {
                batch: 'a code send batch',
                schema: CodeSendBatchRequestSchema,
                body: { send, recipients: [ada, bo] },
                accepted: true,
            },
            {
                batch: 'a repeated idempotency key',
                schema: TemplateBatchRequestSchema,
                body: { recipients: [ada, { ...bo, idempotency_key: ada.idempotency_key }] },
                accepted: false,
            },
            {
                batch: 'a send time for the batch',
                schema: TemplateBatchRequestSchema,
                body: { scheduled_at: '2030-01-01T00:00:00Z', recipients: [ada] },
                accepted: false,
            },
            {
                batch: 'a send time for one recipient',
                schema: TemplateBatchRequestSchema,
                body: { recipients: [ada, { ...bo, scheduled_at: '2030-01-01T00:00:00Z' }] },
                accepted: false,
            },
            {
                batch: 'cc on a code send',
                schema: CodeSendBatchRequestSchema,
                body: { send: { ...send, cc: ['cy@example.com'] }, recipients: [ada] },
                accepted: false,
            },
            {
                batch: 'bcc on a code send',
                schema: CodeSendBatchRequestSchema,
                body: { send: { ...send, bcc: ['cy@example.com'] }, recipients: [ada] },
                accepted: false,
            },
            {
                batch: 'attachments on a code send',
                schema: CodeSendBatchRequestSchema,
                body: { send: { ...send, attachments: [] }, recipients: [ada] },
                accepted: false,
            },
        ])('$batch is accepted: $accepted', ({ schema, body, accepted }) => {
            expect(schema.safeParse(body).success).toBe(accepted)
        })
    })
})
