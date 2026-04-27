import React from 'react';
import ReactPDF from '@react-pdf/renderer';
import ProstheticsReport, { UserReport } from './ProstheticsReport';

async function ReportRenderer(report: UserReport): Promise<NodeJS.ReadableStream> {
    return ReactPDF.renderToStream(<ProstheticsReport report={report} />);
}

export default ReportRenderer;
