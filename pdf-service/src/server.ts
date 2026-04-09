import express, { Express } from 'express';
import reportRouter from './report/reportRouter';

async function createServer(): Promise<Express> {
    const app = express();

    app.use(express.json());

    // Liveness probe
    app.get('/health', (_req, res) => res.send('ok'));

    // PDF report routes: GET /pdf/reports/me
    app.use('/pdf', reportRouter);

    return app;
}

export default createServer;
