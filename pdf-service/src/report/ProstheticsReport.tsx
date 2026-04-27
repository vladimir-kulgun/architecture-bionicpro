import React from 'react';
import { Document, Page, Text, View, StyleSheet } from '@react-pdf/renderer';

// ── Domain types (mirror reports-api/main.go JSON response) ───────────────────

export interface DailyReport {
    date: string;
    total_sessions: number;
    total_active_minutes: number;
    avg_signal_strength_mv: number;
    max_signal_strength_mv: number;
    movement_count: number;
    error_count: number;
    avg_battery_level_pct: number;
    min_battery_level_pct: number;
}

export interface Summary {
    total_days_active: number;
    total_active_minutes: number;
    total_movements: number;
    total_errors: number;
    avg_signal_strength_mv: number;
    avg_battery_level_pct: number;
}

export interface UserReport {
    user_id: string;
    first_name: string;
    last_name: string;
    email: string;
    prosthetics_model: string;
    order_date?: string;
    delivery_date?: string;
    last_service_date?: string;
    period: { from: string; to: string };
    daily_reports: DailyReport[];
    summary: Summary;
}

// ── Styles ─────────────────────────────────────────────────────────────────────

const BLUE   = '#1d4ed8';
const LIGHT  = '#eff6ff';
const GRAY   = '#6b7280';
const RED    = '#dc2626';
const BORDER = '#e5e7eb';
const WHITE  = '#ffffff';
const HEADER_BG = '#3b82f6';

const s = StyleSheet.create({
    page:  { padding: 32, fontSize: 9, color: '#111827', fontFamily: 'Helvetica' },

    // ── Title ──
    titleRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: 12 },
    title:    { fontSize: 20, fontFamily: 'Helvetica-Bold', color: BLUE },
    subtitle: { fontSize: 9, color: GRAY },

    divider:  { borderBottomWidth: 1, borderBottomColor: BORDER, marginBottom: 12 },

    // ── Profile grid ──
    sectionTitle: { fontSize: 11, fontFamily: 'Helvetica-Bold', color: BLUE, marginBottom: 6 },
    infoGrid:     { flexDirection: 'row', flexWrap: 'wrap', marginBottom: 14 },
    infoCell:     { width: '50%', flexDirection: 'row', marginBottom: 4 },
    infoLabel:    { width: 110, color: GRAY, fontFamily: 'Helvetica-Bold' },
    infoValue:    { flex: 1 },

    // ── Summary cards ──
    cardRow:   { flexDirection: 'row', marginBottom: 6 },
    card:      { flex: 1, backgroundColor: LIGHT, borderRadius: 4, padding: 8, marginRight: 6 },
    cardLast:  { flex: 1, backgroundColor: LIGHT, borderRadius: 4, padding: 8 },
    cardErrorBg: { flex: 1, backgroundColor: '#fef2f2', borderRadius: 4, padding: 8, marginRight: 6 },
    cardVal:   { fontSize: 16, fontFamily: 'Helvetica-Bold', color: BLUE, marginBottom: 2 },
    cardValErr:{ fontSize: 16, fontFamily: 'Helvetica-Bold', color: RED,  marginBottom: 2 },
    cardLbl:   { fontSize: 7, color: GRAY },

    // ── Table ──
    tableSection: { marginTop: 14 },
    tableHeader:  { flexDirection: 'row', backgroundColor: HEADER_BG, padding: 4 },
    tableRow:     { flexDirection: 'row', borderBottomWidth: 1, borderBottomColor: BORDER, padding: 4 },
    tableRowAlt:  { flexDirection: 'row', borderBottomWidth: 1, borderBottomColor: BORDER, padding: 4, backgroundColor: '#f9fafb' },
    th: { fontFamily: 'Helvetica-Bold', color: WHITE, fontSize: 7 },
    td: { fontSize: 7 },
    tdErr: { fontSize: 7, color: RED },

    // column flex widths (9 columns, total = 36)
    cDate: { flex: 5 },
    cNum3: { flex: 3, textAlign: 'right' },
    cNum4: { flex: 4, textAlign: 'right' },
});

// ── Sub-components ─────────────────────────────────────────────────────────────

const InfoRow: React.FC<{ label: string; value?: string }> = ({ label, value }) =>
    value ? (
        <View style={s.infoCell}>
            <Text style={s.infoLabel}>{label}</Text>
            <Text style={s.infoValue}>{value}</Text>
        </View>
    ) : null;

interface CardProps { label: string; value: string; error?: boolean; last?: boolean }
const SummaryCard: React.FC<CardProps> = ({ label, value, error, last }) => (
    <View style={error ? s.cardErrorBg : last ? s.cardLast : s.card}>
        <Text style={error ? s.cardValErr : s.cardVal}>{value}</Text>
        <Text style={s.cardLbl}>{label}</Text>
    </View>
);

const TableHeader: React.FC = () => (
    <View style={s.tableHeader} fixed>
        <Text style={[s.th, s.cDate]}>Date</Text>
        <Text style={[s.th, s.cNum3]}>Sessions</Text>
        <Text style={[s.th, s.cNum3]}>Act.min</Text>
        <Text style={[s.th, s.cNum4]}>Signal avg</Text>
        <Text style={[s.th, s.cNum4]}>Signal max</Text>
        <Text style={[s.th, s.cNum3]}>Movements</Text>
        <Text style={[s.th, s.cNum3]}>Errors</Text>
        <Text style={[s.th, s.cNum4]}>Battery avg</Text>
        <Text style={[s.th, s.cNum4]}>Battery min</Text>
    </View>
);

const TableRow: React.FC<{ row: DailyReport; alt: boolean }> = ({ row, alt }) => (
    <View style={alt ? s.tableRowAlt : s.tableRow} wrap={false}>
        <Text style={[s.td, s.cDate]}>{row.date}</Text>
        <Text style={[s.td, s.cNum3]}>{row.total_sessions}</Text>
        <Text style={[s.td, s.cNum3]}>{row.total_active_minutes}</Text>
        <Text style={[s.td, s.cNum4]}>{row.avg_signal_strength_mv.toFixed(1)}</Text>
        <Text style={[s.td, s.cNum4]}>{row.max_signal_strength_mv.toFixed(1)}</Text>
        <Text style={[s.td, s.cNum3]}>{row.movement_count}</Text>
        <Text style={[row.error_count > 0 ? s.tdErr : s.td, s.cNum3]}>{row.error_count}</Text>
        <Text style={[s.td, s.cNum4]}>{row.avg_battery_level_pct.toFixed(1)}</Text>
        <Text style={[s.td, s.cNum4]}>{row.min_battery_level_pct.toFixed(1)}</Text>
    </View>
);

// ── Root document ──────────────────────────────────────────────────────────────

const ProstheticsReport: React.FC<{ report: UserReport }> = ({ report }) => {
    const { summary: sm, daily_reports: days, period } = report;

    return (
        <Document title="Prosthetics Report" author="BionicPRO">
            <Page size="A4" style={s.page}>

                {/* Title */}
                <View style={s.titleRow}>
                    <Text style={s.title}>Prosthetics Report</Text>
                    <Text style={s.subtitle}>BionicPRO · {period.from} — {period.to}</Text>
                </View>
                <View style={s.divider} />

                {/* Profile */}
                <Text style={s.sectionTitle}>User Information</Text>
                <View style={s.infoGrid}>
                    <InfoRow label="User"           value={`${report.first_name} ${report.last_name}`} />
                    <InfoRow label="Email"          value={report.email} />
                    <InfoRow label="Prosthetic model" value={report.prosthetics_model} />
                    <InfoRow label="Delivery date"  value={report.delivery_date} />
                    <InfoRow label="Last service"   value={report.last_service_date} />
                    <InfoRow label="Order date"     value={report.order_date} />
                </View>
                <View style={s.divider} />

                {/* Summary */}
                <Text style={s.sectionTitle}>Period Summary</Text>
                <View style={s.cardRow}>
                    <SummaryCard label="Active days"    value={String(sm.total_days_active)} />
                    <SummaryCard label="Active minutes" value={String(sm.total_active_minutes)} />
                    <SummaryCard label="Movements"      value={String(sm.total_movements)} last />
                </View>
                <View style={[s.cardRow, { marginBottom: 14 }]}>
                    <SummaryCard label="Errors"         value={String(sm.total_errors)}
                                 error={sm.total_errors > 0} />
                    <SummaryCard label="Avg signal, mV" value={sm.avg_signal_strength_mv.toFixed(1)} />
                    <SummaryCard label="Avg battery, %" value={sm.avg_battery_level_pct.toFixed(1)} last />
                </View>
                <View style={s.divider} />

                {/* Daily table */}
                <View style={s.tableSection}>
                    <Text style={[s.sectionTitle, { marginBottom: 6 }]}>Daily Data</Text>
                    {days.length === 0 ? (
                        <Text style={{ color: GRAY }}>No data for the selected period.</Text>
                    ) : (
                        <View>
                            <TableHeader />
                            {days.map((row, i) => (
                                <TableRow key={row.date} row={row} alt={i % 2 === 1} />
                            ))}
                        </View>
                    )}
                </View>

            </Page>
        </Document>
    );
};

export default ProstheticsReport;
