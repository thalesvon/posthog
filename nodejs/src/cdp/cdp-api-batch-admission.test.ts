import { createMockJobQueue } from '../../tests/helpers/mocks/job-queue.mock'

import { Server } from 'http'
import jwt from 'jsonwebtoken'
import supertest from 'supertest'
import express from 'ultimate-express'

import { setupExpressApp } from '~/common/api/router'
import { closeHub, createHub } from '~/common/utils/db/hub'

import { createCdpConsumerDeps } from '../../tests/helpers/cdp'
import { Hub } from '../types'
import { CdpApi } from './cdp-api'

describe('a transactional email batch on a pod that requires the internal secret, with no request to spare', () => {
    const TEMPLATE = 'transactional_email/batch'
    const CODE = 'transactional_email/send/batch'
    let hub: Hub
    let api: CdpApi
    let app: express.Application
    let server: Server

    beforeAll(async () => {
        hub = await createHub({
            SITE_URL: 'http://localhost:8000',
            CDP_TRANSACTIONAL_EMAIL_BATCH_MAX_REQUESTS: 0,
            CDP_INTAKE_KAFKA_ASYNC_TEAMS: '1',
        })
        api = new CdpApi(hub, createCdpConsumerDeps(hub), {
            hogQueue: createMockJobQueue(),
            hogflowQueue: createMockJobQueue(),
        })
        app = setupExpressApp({ jsonBodyLimit: '1kb', internalApiSecret: 'shared-secret' })
        app.use('/', api.router())
        server = app.listen(0, () => {})
    })

    afterAll(async () => {
        server.close()
        await api.stop()
        await closeHub(hub)
    })

    const tokenOfKind = (kind: string | null): Record<string, string> =>
        kind === null
            ? {}
            : {
                  Authorization: `Bearer ${jwt.sign(
                      { team_id: 1, hog_flow_id: 'some-flow', kind },
                      hub.WORKFLOWS_TRANSACTIONAL_EMAIL_JWT_SECRET,
                      { audience: 'posthog:workflows:send_transactional_email', expiresIn: '2m' }
                  )}`,
              }

    // The cap is zero, so a request that passes its token check is refused as over capacity. A refusal for its token
    // proves the check ran before admission. The body is over the parser's limit, so a 503 also proves admission ran
    // before the body was parsed.
    it.each([
        [TEMPLATE, '', 'template_batch', 503],
        [TEMPLATE, '/', 'template_batch', 503],
        [CODE, '', 'code_send_batch', 503],
        [CODE, '/', 'code_send_batch', 503],
        [TEMPLATE, '', null, 401],
        [TEMPLATE, '/', null, 401],
        [CODE, '', null, 401],
        [CODE, '/', null, 401],
        [TEMPLATE, '/', 'code_send_batch', 401],
        [CODE, '/', 'template_batch', 401],
    ])('answers a request to %s%s with token kind %s with %s', async (route, suffix, kind, status) => {
        const res = await supertest(app)
            .post(`/api/projects/1/hog_flows/some-flow/${route}${suffix}`)
            .set(tokenOfKind(kind))
            .send({ variables: { body: 'x'.repeat(2048) }, recipients: [] })

        expect(res.status).toEqual(status)
        if (status === 503) {
            expect(res.body).toEqual({ error: 'over_capacity' })
        }
    })
})
