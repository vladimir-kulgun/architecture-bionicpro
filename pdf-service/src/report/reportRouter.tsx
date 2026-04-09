import { Router, Request, Response } from 'express';
import { UserReport } from './ProstheticsReport';
import ReportRenderer from './ReportRenderer';

const router = Router();

// POST /render
//
// Pure rendering endpoint: accepts a pre-built UserReport JSON body from
// reports-api (the orchestrator) and returns a PDF stream.
//
// reports-api is responsible for:
//   - reading data from ClickHouse
//   - enforcing auth / RBAC
//   - calling this endpoint with the assembled report
//
// This service is internal-only and is not exposed through the BFF.
router.post('/render', async (req: Request, res: Response) => {
    try {
        const report = req.body as UserReport;

        if (!report?.user_id || !report?.period) {
            res.status(400).send('invalid report body');
            return;
        }

        // Render React-PDF components to a stream and pipe to the response.
        const pdfStream = await ReportRenderer(report);

        const filename = `report_${report.period.from}_${report.period.to}.pdf`;
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);

        pdfStream.pipe(res);
    } catch (err: any) {
        console.error('pdf render error:', err);
        res.status(500).send(err?.message ?? 'internal server error');
    }
});

export default router;
