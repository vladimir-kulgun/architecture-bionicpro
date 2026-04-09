import React from 'react';
import ReactPDF from '@react-pdf/renderer';
import ProstheticsReport, { UserReport } from './ProstheticsReport';

// Mirrors the pattern from TalentMesh pdf-reports-prototype/src/reports/ReportRenderer.tsx:
// compose React-PDF components from domain data, then render to a Node.js readable stream.
async function ReportRenderer(report: UserReport): Promise<NodeJS.ReadableStream> {
    return ReactPDF.renderToStream(<ProstheticsReport report={report} />);
}

export default ReportRenderer;
