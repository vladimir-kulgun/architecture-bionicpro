import React, { useState, useEffect } from 'react';

const AUTH_URL = process.env.REACT_APP_AUTH_URL || 'http://localhost:8001';

// ── API response types (mirror reports-api/main.go JSON fields) ───────────────

interface DailyReport {
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

interface Summary {
  total_days_active: number;
  total_active_minutes: number;
  total_movements: number;
  total_errors: number;
  avg_signal_strength_mv: number;
  avg_battery_level_pct: number;
}

interface UserReport {
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

// ── Date helpers ──────────────────────────────────────────────────────────────

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function defaultFrom(): string {
  const d = new Date();
  d.setDate(d.getDate() - 30);
  return isoDate(d);
}

function defaultTo(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return isoDate(d);
}

// Airflow DAG runs at 02:00 UTC and processes data for the previous day only.
// Dates from today onward are never in ClickHouse.
const MAX_DATE = defaultTo(); // yesterday

// ── Component ─────────────────────────────────────────────────────────────────

const ReportPage: React.FC = () => {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [loading, setLoading]             = useState(false);
  const [error, setError]                 = useState<string | null>(null);
  const [report, setReport]               = useState<UserReport | null>(null);
  const [from, setFrom]                   = useState(defaultFrom);
  const [to, setTo]                       = useState(defaultTo);

  useEffect(() => {
    // Check whether an active session exists (session cookie is sent automatically).
    fetch(`${AUTH_URL}/auth/session`, { credentials: 'include' })
      .then(r => setAuthenticated(r.ok))
      .catch(() => setAuthenticated(false));
  }, []);

  const fetchReport = async () => {
    try {
      setLoading(true);
      setError(null);

      // Session cookie is attached automatically by the browser.
      // bionicpro-auth validates the session, injects the Bearer token, and
      // proxies the request to GET /reports/me on the upstream Reports API.
      const response = await fetch(
        `${AUTH_URL}/api/reports/me?from=${from}&to=${to}`,
        { credentials: 'include' },
      );

      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }

      if (!response.ok) {
        throw new Error(`Ошибка сервера: ${response.status}`);
      }

      const data: UserReport = await response.json();
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Произошла ошибка');
    } finally {
      setLoading(false);
    }
  };

  // ── Auth states ─────────────────────────────────────────────────────────────

  if (authenticated === null) {
    return (
      <div className="flex items-center justify-center min-h-screen text-gray-500">
        Загрузка...
      </div>
    );
  }

  if (!authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => { window.location.href = `${AUTH_URL}/auth/login`; }}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Войти
        </button>
      </div>
    );
  }

  // ── Main view ───────────────────────────────────────────────────────────────

  return (
    <div className="min-h-screen bg-gray-100 py-10 px-4">
      <div className="max-w-4xl mx-auto">

        {/* ── Header card ── */}
        <div className="bg-white rounded-lg shadow-md p-6 mb-6">
          <h1 className="text-2xl font-bold mb-4">Отчёт по протезу</h1>

          {/* Date range controls */}
          <div className="flex flex-wrap items-end gap-4">
            <label className="flex flex-col text-sm text-gray-600">
              С
              <input
                type="date"
                value={from}
                max={to}
                onChange={e => setFrom(e.target.value)}
                className="mt-1 border border-gray-300 rounded px-2 py-1 text-gray-800"
              />
            </label>

            <label className="flex flex-col text-sm text-gray-600">
              По
              <input
                type="date"
                value={to}
                min={from}
                max={MAX_DATE}
                onChange={e => setTo(e.target.value)}
                className="mt-1 border border-gray-300 rounded px-2 py-1 text-gray-800"
              />
            </label>

            <button
              onClick={fetchReport}
              disabled={loading}
              className={`px-5 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 font-medium ${
                loading ? 'opacity-50 cursor-not-allowed' : ''
              }`}
            >
              {loading ? 'Загрузка...' : 'Получить отчёт'}
            </button>
          </div>

          <p className="text-xs text-gray-400 mt-3">
            Данные доступны по {MAX_DATE} включительно — Airflow обрабатывает данные за предыдущий день.
          </p>

          {error && (
            <div className="mt-4 p-3 bg-red-100 text-red-700 rounded text-sm">
              {error}
            </div>
          )}
        </div>

        {/* ── Report content (rendered after successful fetch) ── */}
        {report && (
          <>
            {/* Profile */}
            <div className="bg-white rounded-lg shadow-md p-6 mb-6">
              <h2 className="text-lg font-semibold mb-3 text-gray-700">Информация о пользователе</h2>
              <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
                <div><span className="text-gray-500">Имя:</span>{' '}
                  <span className="font-medium">{report.first_name} {report.last_name}</span>
                </div>
                <div><span className="text-gray-500">Email:</span>{' '}
                  <span>{report.email}</span>
                </div>
                <div><span className="text-gray-500">Модель протеза:</span>{' '}
                  <span className="font-medium">{report.prosthetics_model}</span>
                </div>
                {report.delivery_date && (
                  <div><span className="text-gray-500">Дата поставки:</span>{' '}
                    <span>{report.delivery_date}</span>
                  </div>
                )}
                {report.last_service_date && (
                  <div><span className="text-gray-500">Последнее ТО:</span>{' '}
                    <span>{report.last_service_date}</span>
                  </div>
                )}
                <div><span className="text-gray-500">Период отчёта:</span>{' '}
                  <span>{report.period.from} — {report.period.to}</span>
                </div>
              </div>
            </div>

            {/* Summary cards */}
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-6">
              <SummaryCard label="Активных дней"  value={String(report.summary.total_days_active)} />
              <SummaryCard label="Акт. минут"     value={String(report.summary.total_active_minutes)} />
              <SummaryCard label="Движений"       value={String(report.summary.total_movements)} />
              <SummaryCard label="Ошибок"         value={String(report.summary.total_errors)}
                           highlight={report.summary.total_errors > 0} />
              <SummaryCard label="Сигнал (ср.), мВ"  value={report.summary.avg_signal_strength_mv.toFixed(1)} />
              <SummaryCard label="Батарея (ср.), %"  value={report.summary.avg_battery_level_pct.toFixed(1)} />
            </div>

            {/* Daily table */}
            {report.daily_reports.length > 0 ? (
              <div className="bg-white rounded-lg shadow-md p-6 overflow-x-auto">
                <h2 className="text-lg font-semibold mb-4 text-gray-700">Данные по дням</h2>
                <table className="w-full text-sm text-left">
                  <thead>
                    <tr className="border-b text-gray-500 text-xs uppercase">
                      <th className="pb-2 pr-4">Дата</th>
                      <th className="pb-2 pr-4 text-right">Сессий</th>
                      <th className="pb-2 pr-4 text-right">Акт. мин</th>
                      <th className="pb-2 pr-4 text-right">Сигнал ср/макс</th>
                      <th className="pb-2 pr-4 text-right">Движений</th>
                      <th className="pb-2 pr-4 text-right">Ошибок</th>
                      <th className="pb-2 text-right">Батарея ср/мин</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.daily_reports.map(row => (
                      <tr key={row.date} className="border-b last:border-0 hover:bg-gray-50">
                        <td className="py-2 pr-4 font-medium">{row.date}</td>
                        <td className="py-2 pr-4 text-right">{row.total_sessions}</td>
                        <td className="py-2 pr-4 text-right">{row.total_active_minutes}</td>
                        <td className="py-2 pr-4 text-right">
                          {row.avg_signal_strength_mv.toFixed(1)} / {row.max_signal_strength_mv.toFixed(1)}
                        </td>
                        <td className="py-2 pr-4 text-right">{row.movement_count}</td>
                        <td className={`py-2 pr-4 text-right ${row.error_count > 0 ? 'text-red-600 font-medium' : ''}`}>
                          {row.error_count}
                        </td>
                        <td className="py-2 text-right">
                          {row.avg_battery_level_pct.toFixed(1)} / {row.min_battery_level_pct.toFixed(1)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="bg-white rounded-lg shadow-md p-6 text-center text-gray-500 text-sm">
                За выбранный период данных нет.
              </div>
            )}
          </>
        )}

      </div>
    </div>
  );
};

// ── Small helper component ────────────────────────────────────────────────────

interface SummaryCardProps {
  label: string;
  value: string;
  highlight?: boolean;
}

const SummaryCard: React.FC<SummaryCardProps> = ({ label, value, highlight }) => (
  <div className="bg-white rounded-lg shadow-md p-4 text-center">
    <div className={`text-2xl font-bold mb-1 ${highlight ? 'text-red-500' : 'text-blue-600'}`}>
      {value}
    </div>
    <div className="text-xs text-gray-500 uppercase tracking-wide">{label}</div>
  </div>
);

export default ReportPage;
