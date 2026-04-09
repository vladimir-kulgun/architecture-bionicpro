import React, { useState, useEffect } from 'react';

const AUTH_URL = process.env.REACT_APP_AUTH_URL || 'http://localhost:8001';

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
  const [from, setFrom]                   = useState(defaultFrom);
  const [to, setTo]                       = useState(defaultTo);

  useEffect(() => {
    // Check whether an active session exists (session cookie is sent automatically).
    fetch(`${AUTH_URL}/auth/session`, { credentials: 'include' })
      .then(r => setAuthenticated(r.ok))
      .catch(() => setAuthenticated(false));
  }, []);

  const downloadReport = async () => {
    try {
      setLoading(true);
      setError(null);

      // Session cookie is attached automatically by the browser.
      // bionicpro-auth validates the session, injects the Bearer token, and
      // proxies the request to GET /pdf/reports/me on pdf-service.
      // pdf-service fetches JSON from reports-api and renders it via @react-pdf/renderer.
      const response = await fetch(
        `${AUTH_URL}/api/reports/me/pdf?from=${from}&to=${to}`,
        { credentials: 'include' },
      );

      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }

      if (!response.ok) {
        const msg = await response.text();
        throw new Error(msg || `Ошибка сервера: ${response.status}`);
      }

      // API returns a CDN URL; navigate to it so the browser downloads the PDF.
      const { url } = await response.json();
      const a       = document.createElement('a');
      a.href        = url;
      a.target      = '_blank';
      a.rel         = 'noopener noreferrer';
      a.click();
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
    <div className="min-h-screen bg-gray-100 flex items-center justify-center px-4">
      <div className="bg-white rounded-lg shadow-md p-8 w-full max-w-md">
        <h1 className="text-2xl font-bold mb-6">Отчёт по протезу</h1>

        {/* Date range controls */}
        <div className="flex flex-col gap-4 mb-4">
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
        </div>

        <p className="text-xs text-gray-400 mb-5">
          Данные доступны по {MAX_DATE} включительно — Airflow обрабатывает данные за предыдущий день.
        </p>

        {/* PDF download button */}
        <button
          onClick={downloadReport}
          disabled={loading}
          className={`w-full px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 font-medium ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Формирование PDF...' : 'Скачать отчёт (PDF)'}
        </button>

        {error && (
          <div className="mt-4 p-3 bg-red-100 text-red-700 rounded text-sm">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
