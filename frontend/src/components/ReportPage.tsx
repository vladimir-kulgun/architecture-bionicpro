import React, { useState, useEffect } from 'react';

const AUTH_URL = process.env.REACT_APP_AUTH_URL || 'http://localhost:8001';

const ReportPage: React.FC = () => {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
      // proxies the request to the upstream API.
      const response = await fetch(`${AUTH_URL}/api/reports`, {
        credentials: 'include',
      });

      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }

      if (!response.ok) {
        throw new Error(`Server error: ${response.status}`);
      }

      // Download the report file returned by the API.
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'report.pdf';
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (authenticated === null) {
    return <div className="flex items-center justify-center min-h-screen">Loading...</div>;
  }

  if (!authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => { window.location.href = `${AUTH_URL}/auth/login`; }}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">{error}</div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
