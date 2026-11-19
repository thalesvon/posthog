import { Semaphore } from '../../utils/sempahore'
import {
    BatchCapacity,
    CodeSendBatchRequestSchema,
    SlotReply,
    TemplateBatchRequestSchema,
    batchDeadlineMs,
    runBatchSlots,
} from './transactional-email-batch'

describe('runBatchSlots', () => {
    const queued = (id: string): SlotReply => ({ outcome: 'queued', status: 202, body: { invocation_id: id } })

    const gate = () => {
        let open: () => void = () => {}
        const opened = new Promise<void>((resolve) => (open = resolve))
        return { open, opened }
    }

    const isSettled = async (promise: Promise<unknown>): Promise<boolean> => {
        let settled = false
        void promise.then(() => (settled = true))
        await new Promise((resolve) => setImmediate(resolve))
        return settled
    }

    it('answers at the deadline, and holds the permit of a recipient still running until it settles', async () => {
        const slots = new Semaphore(1)
        const deadline = new AbortController()
        const stuck = gate()
        const ran: string[] = []

        const answered = runBatchSlots(
            ['a', 'b', 'c'],
            async (item) => {
                ran.push(item)
                if (item === 'b') {
                    deadline.abort()
                    await stuck.opened
                }
                return queued(item)
            },
            { slots, fanOut: 1, deadline: deadline.signal }
        )
        const { replies, settled } = await answered

        expect(replies).toEqual([
            queued('a'),
            {
                outcome: 'deadline_running',
                status: 503,
                body: { error: 'delivery_unavailable', may_still_go_out: true },
            },
            { outcome: 'not_attempted', status: 503, body: { error: 'not_attempted' } },
        ])
        expect(await isSettled(settled)).toBe(false)
        expect(await isSettled(slots.run(() => Promise.resolve()))).toBe(false)

        stuck.open()

        expect(await isSettled(settled)).toBe(true)
        expect(ran).toEqual(['a', 'b'])
    })

    it.each([
        ['before it could queue the email', false, false],
        ['after it could have queued the email', true, true],
    ])('answers only its own recipient when a send throws %s', async (_case, committing, mayStillGoOut) => {
        const { replies } = await runBatchSlots(
            ['a', 'b'],
            (item, slot) => {
                if (item === 'b') {
                    return Promise.resolve(queued(item))
                }
                if (committing) {
                    slot.mayCommit()
                }
                return Promise.reject(new Error('connection reset'))
            },
            { slots: new Semaphore(100), fanOut: 25, deadline: new AbortController().signal }
        )

        expect(replies).toEqual([
            { outcome: 'error', status: 503, body: { error: 'delivery_unavailable', may_still_go_out: mayStillGoOut } },
            queued('b'),
        ])
    })

    it('runs at most fanOut recipients of one batch at once', async () => {
        let running = 0
        let mostRunning = 0
        const { replies } = await runBatchSlots(
            Array.from({ length: 60 }, (_, i) => String(i)),
            async (item) => {
                mostRunning = Math.max(mostRunning, ++running)
                await new Promise((resolve) => setImmediate(resolve))
                running--
                return queued(item)
            },
            { slots: new Semaphore(100), fanOut: 25, deadline: new AbortController().signal }
        )

        expect(replies).toHaveLength(60)
        expect(replies.every((reply) => reply.outcome === 'queued')).toBe(true)
        expect(mostRunning).toBe(25)
    })
})

describe('BatchCapacity', () => {
    it('refuses a request over the cap until an admitted one releases', () => {
        const capacity = new BatchCapacity(1, 100)
        const release = capacity.admit()

        expect(release).not.toBeNull()
        expect(capacity.admit()).toBeNull()

        release!()
        release!()

        expect(capacity.admit()).not.toBeNull()
        expect(capacity.admit()).toBeNull()
    })
})

describe('batchDeadlineMs', () => {
    it.each([-1, 0, 2.5, 25_001, 29_999])('refuses a deadline of %s ms', (setting) => {
        expect(() => batchDeadlineMs(setting)).toThrow('CDP_TRANSACTIONAL_EMAIL_BATCH_DEADLINE_MS')
    })

    it('keeps a deadline that leaves Django time to read the answer', () => {
        expect(batchDeadlineMs(25_000)).toBe(25_000)
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
        const parsed = TemplateBatchRequestSchema.safeParse({ recipients: Array.from({ length: 1000 }, () => ({})) })

        expect(parsed.error?.issues).toEqual([expect.objectContaining({ code: 'too_big', path: ['recipients'] })])
    })
})

describe('whole-batch refusals', () => {
    const ada = { to: 'ada@example.com', distinct_id: 'person-ada', idempotency_key: 'key-ada' }
    const bo = { to: 'bo@example.com', distinct_id: 'person-bo', idempotency_key: 'key-bo' }
    const send = { content: { subject: 'Hello', text: 'Hi' }, tracking: { captureEngagement: false } }

    it.each([
        {
            batch: 'a template batch',
            schema: TemplateBatchRequestSchema,
            body: { recipients: [ada, bo] },
            accepted: true,
        },
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
