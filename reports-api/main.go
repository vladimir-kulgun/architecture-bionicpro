// reports-api — BionicPRO reporting backend.
//
// Serves pre-computed prosthetics usage reports from the ClickHouse OLAP
// data mart (table user_prosthetics_report, populated by Airflow ETL).
// No real-time computation: every request is a simple point-lookup by
// user_id + date range on a pre-aggregated table.
//
// Authentication:
//   The BFF (bionicpro-auth) validates the Keycloak session and forwards
//   all /api/* requests with the user's Bearer JWT. This service parses
//   JWT claims (sub, realm_access.roles) without re-verifying the signature,
//   trusting the upstream BFF as the auth boundary.
//
// Access control — two separate routes with different trust models:
//
//   GET /reports/me              — self-service; user_id is always taken from the
//                                  JWT sub claim, never from the URL. IDOR is
//                                  architecturally impossible on this route.
//                                  Required role: prothetic_user OR administrator.
//
//   GET /reports/{user_id}       — admin lookup; allows fetching any user's report.
//                                  Required role: administrator (enforced by
//                                  requireAdmin middleware before the handler runs).
//                                  prothetic_user receives 403 even if they happen
//                                  to know another user's Keycloak ID.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ── Configuration ──────────────────────────────────────────────────────────────

type config struct {
	listenAddr    string
	clickhouseURL string
	frontendURL   string
	pdfServiceURL string // pdf-service renders JSON → PDF
	s3Endpoint    string // MinIO/S3 host:port, e.g. minio:9000
	s3Bucket      string // bucket for cached PDFs
	s3AccessKey   string
	s3SecretKey   string
	cdnURL        string // public base URL of the CDN (Nginx proxy), e.g. http://localhost:9080
}

func loadConfig() config {
	return config{
		listenAddr:    ":" + getenv("PORT", "8000"),
		clickhouseURL: getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
		frontendURL:   getenv("FRONTEND_URL", "http://localhost:3000"),
		pdfServiceURL: getenv("PDF_SERVICE_URL", "http://pdf-service:5501"),
		s3Endpoint:    getenv("S3_ENDPOINT", ""),
		s3Bucket:      getenv("S3_BUCKET", "reports"),
		s3AccessKey:   getenv("S3_ACCESS_KEY", ""),
		s3SecretKey:   getenv("S3_SECRET_KEY", ""),
		cdnURL:        getenv("CDN_URL", "http://localhost:9080"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── JWT ────────────────────────────────────────────────────────────────────────

type jwtClaims struct {
	Sub               string `json:"sub"`
	PreferredUsername string `json:"preferred_username"`
	RealmAccess       struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

func (c *jwtClaims) hasRole(role string) bool {
	for _, r := range c.RealmAccess.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// parseJWT decodes the payload of a Bearer JWT and returns its claims.
// Signature verification is intentionally skipped: the BFF (bionicpro-auth)
// is responsible for validating tokens against Keycloak before proxying.
func parseJWT(authHeader string) (*jwtClaims, error) {
	token := strings.TrimPrefix(authHeader, "Bearer ")
	if token == authHeader {
		return nil, fmt.Errorf("missing Bearer prefix")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed JWT: expected 3 parts, got %d", len(parts))
	}

	// JWT payload is the second segment, base64url-encoded with no padding.
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}

	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	if claims.Sub == "" {
		return nil, fmt.Errorf("JWT missing 'sub' claim")
	}
	return &claims, nil
}

// ── ClickHouse client ──────────────────────────────────────────────────────────

// chQuery sends SQL to ClickHouse via HTTP POST and returns the raw response.
// SELECT queries should end with "FORMAT JSONEachRow".
func chQuery(chURL, sql string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, chURL, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse HTTP %d: %s", resp.StatusCode, body)
	}
	return body, nil
}

// ── Domain types ───────────────────────────────────────────────────────────────

// chRow mirrors the columns of user_prosthetics_report as returned by
// ClickHouse FORMAT JSONEachRow. JSON tag names must match column/alias names.
type chRow struct {
	UserID           string  `json:"user_id"`
	ReportDate       string  `json:"report_date"`
	FirstName        string  `json:"first_name"`
	LastName         string  `json:"last_name"`
	Email            string  `json:"email"`
	ProstheticsModel string  `json:"prosthetics_model"`
	OrderDate        string  `json:"order_date"`
	DeliveryDate     string  `json:"delivery_date"`
	LastServiceDate  string  `json:"last_service_date"`
	TotalSessions    uint32  `json:"total_sessions"`
	TotalActiveMin   uint32  `json:"total_active_min"`
	AvgSignal        float32 `json:"avg_signal_strength"`
	MaxSignal        float32 `json:"max_signal_strength"`
	MovementCount    uint32  `json:"movement_count"`
	ErrorCount       uint32  `json:"error_count"`
	AvgBattery       float32 `json:"avg_battery_level"`
	MinBattery       float32 `json:"min_battery_level"`
}

// DailyReport is one row in the report's timeline.
type DailyReport struct {
	Date               string  `json:"date"`
	TotalSessions      uint32  `json:"total_sessions"`
	TotalActiveMinutes uint32  `json:"total_active_minutes"`
	AvgSignalStrength  float32 `json:"avg_signal_strength_mv"`
	MaxSignalStrength  float32 `json:"max_signal_strength_mv"`
	MovementCount      uint32  `json:"movement_count"`
	ErrorCount         uint32  `json:"error_count"`
	AvgBatteryLevel    float32 `json:"avg_battery_level_pct"`
	MinBatteryLevel    float32 `json:"min_battery_level_pct"`
}

// Summary contains aggregated metrics across the entire requested period.
type Summary struct {
	TotalDaysActive    int     `json:"total_days_active"`
	TotalActiveMinutes uint32  `json:"total_active_minutes"`
	TotalMovements     uint32  `json:"total_movements"`
	TotalErrors        uint32  `json:"total_errors"`
	AvgSignalStrength  float32 `json:"avg_signal_strength_mv"`
	AvgBatteryLevel    float32 `json:"avg_battery_level_pct"`
}

// Period holds the report's time window as ISO-8601 date strings.
type Period struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// UserReport is the top-level API response for both /reports/me and /reports/{user_id}.
type UserReport struct {
	UserID           string        `json:"user_id"`
	FirstName        string        `json:"first_name"`
	LastName         string        `json:"last_name"`
	Email            string        `json:"email"`
	ProstheticsModel string        `json:"prosthetics_model"`
	OrderDate        string        `json:"order_date,omitempty"`
	DeliveryDate     string        `json:"delivery_date,omitempty"`
	LastServiceDate  string        `json:"last_service_date,omitempty"`
	Period           Period        `json:"period"`
	DailyReports     []DailyReport `json:"daily_reports"`
	Summary          Summary       `json:"summary"`
}

// ── Server ─────────────────────────────────────────────────────────────────────

type server struct {
	cfg config
	log *slog.Logger
	s3  *minio.Client // nil when S3 is not configured
}

func newServer(cfg config) *server {
	srv := &server{cfg: cfg, log: slog.Default()}
	if cfg.s3Endpoint != "" && cfg.s3AccessKey != "" {
		mc, err := minio.New(cfg.s3Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.s3AccessKey, cfg.s3SecretKey, ""),
			Secure: false,
		})
		if err != nil {
			slog.Warn("minio init failed, S3 cache disabled", "err", err)
		} else {
			srv.s3 = mc
			srv.ensureS3Bucket()
		}
	}
	return srv
}

// ensureS3Bucket creates the PDF cache bucket if it does not yet exist and
// sets a public-read policy so Nginx can proxy objects without credentials.
func (s *server) ensureS3Bucket() {
	ctx := context.Background()
	exists, err := s.s3.BucketExists(ctx, s.cfg.s3Bucket)
	if err != nil {
		s.log.Warn("s3 BucketExists failed", "err", err)
		return
	}
	if !exists {
		if err := s.s3.MakeBucket(ctx, s.cfg.s3Bucket, minio.MakeBucketOptions{}); err != nil {
			s.log.Warn("s3 MakeBucket failed", "err", err)
			return
		}
		s.log.Info("s3 bucket created", "bucket", s.cfg.s3Bucket)
	}

	// Allow anonymous GET so Nginx CDN can proxy without S3 credentials.
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::` + s.cfg.s3Bucket + `/*"]}]}`
	if err := s.s3.SetBucketPolicy(ctx, s.cfg.s3Bucket, policy); err != nil {
		s.log.Warn("s3 SetBucketPolicy failed", "err", err)
	}
}

// ── Middleware ─────────────────────────────────────────────────────────────────

// requireAuth parses the Bearer JWT and stores claims in the request header
// X-JWT-Sub and X-JWT-Roles for downstream handlers. Returns 401 on failure.
//
// All protected routes must be wrapped with this middleware before any role check.
func (s *server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, err := parseJWT(r.Header.Get("Authorization"))
		if err != nil {
			s.log.Warn("auth failed", "err", err, "ip", r.RemoteAddr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Propagate claims to the handler via request headers.
		// This avoids context values and keeps the chain explicit.
		r.Header.Set("X-JWT-Sub", claims.Sub)
		r.Header.Set("X-JWT-Username", claims.PreferredUsername)
		r.Header.Set("X-JWT-Roles", strings.Join(claims.RealmAccess.Roles, ","))
		next(w, r)
	}
}

// requireAdmin wraps requireAuth and additionally enforces the administrator role.
// Returns 403 if the authenticated user lacks the role.
//
// Usage: mux.HandleFunc("GET /reports/{user_id}", srv.requireAdmin(srv.handleAdminGetReport))
func (s *server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		if !hasRole(r, "administrator") {
			s.log.Warn("admin route accessed without role",
				"sub", r.Header.Get("X-JWT-Sub"),
				"path", r.URL.Path,
			)
			http.Error(w, "forbidden: administrator role required", http.StatusForbidden)
			return
		}
		next(w, r)
	})
}

// hasRole checks whether the X-JWT-Roles header (set by requireAuth) contains role.
func hasRole(r *http.Request, role string) bool {
	for _, v := range strings.Split(r.Header.Get("X-JWT-Roles"), ",") {
		if strings.TrimSpace(v) == role {
			return true
		}
	}
	return false
}

// ── Handlers ───────────────────────────────────────────────────────────────────

// GET /reports/me?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Self-service endpoint: always returns the authenticated user's own report.
// The user_id is taken exclusively from the JWT sub claim — the URL carries
// no identifier, so there is no parameter to forge (IDOR is impossible).
//
// Required role: prothetic_user OR administrator (any authenticated user).
func (s *server) handleMyReport(w http.ResponseWriter, r *http.Request) {
	// user_id is always the caller's own Keycloak subject — never from the URL.
	userID := r.Header.Get("X-JWT-Sub")

	if !hasRole(r, "prothetic_user") && !hasRole(r, "administrator") {
		http.Error(w, "forbidden: prothetic_user or administrator role required", http.StatusForbidden)
		return
	}

	from, to, ok := parseDateRange(w, r)
	if !ok {
		return
	}

	s.serveReport(w, r, userID, from, to)
}

// GET /reports/{user_id}?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Admin lookup: allows fetching any user's report by their Keycloak ID.
// This handler is only reachable through the requireAdmin middleware, which
// returns 403 before this code runs if the caller lacks the administrator role.
//
// Required role: administrator (enforced by middleware, not by this handler).
func (s *server) handleAdminGetReport(w http.ResponseWriter, r *http.Request) {
	targetUserID := r.PathValue("user_id")
	if targetUserID == "" {
		http.Error(w, "missing user_id path parameter", http.StatusBadRequest)
		return
	}

	from, to, ok := parseDateRange(w, r)
	if !ok {
		return
	}

	s.log.Info("admin report lookup",
		"admin_sub", r.Header.Get("X-JWT-Sub"),
		"target_user_id", targetUserID,
	)

	s.serveReport(w, r, targetUserID, from, to)
}

// fetchReport queries ClickHouse and assembles a UserReport.
// It is the single source of report data, shared by the JSON and PDF handlers.
func (s *server) fetchReport(userID, from, to string) (*UserReport, error) {
	// FINAL forces ReplacingMergeTree deduplication before the query runs,
	// guaranteeing that the client always receives the latest ETL version of
	// each (user_id, report_date) pair even when background merges are pending.
	sql := fmt.Sprintf(`
SELECT
    user_id,
    toString(report_date)   AS report_date,
    first_name, last_name, email,
    prosthetics_model,
    toString(order_date)    AS order_date,
    delivery_date,
    last_service_date,
    total_sessions,
    total_active_min,
    avg_signal_strength,
    max_signal_strength,
    movement_count,
    error_count,
    avg_battery_level,
    min_battery_level
FROM user_prosthetics_report FINAL
WHERE user_id     = '%s'
  AND report_date >= '%s'
  AND report_date <= '%s'
ORDER BY report_date
FORMAT JSONEachRow`,
		escapeString(userID),
		from,
		to,
	)

	body, err := chQuery(s.cfg.clickhouseURL, sql)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}

	// ClickHouse JSONEachRow: one JSON object per newline.
	var rows []chRow
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row chRow
		if err := json.Unmarshal(line, &row); err != nil {
			s.log.Error("parse row", "err", err)
			continue
		}
		rows = append(rows, row)
	}

	return buildReport(userID, from, to, rows), nil
}

// serveReport fetches data from ClickHouse and writes a JSON response.
func (s *server) serveReport(w http.ResponseWriter, r *http.Request, userID, from, to string) {
	report, err := s.fetchReport(userID, from, to)
	if err != nil {
		s.log.Error("clickhouse query failed", "user_id", userID, "err", err)
		http.Error(w, "failed to fetch report data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		s.log.Error("encode response", "err", err)
	}

	s.log.Info("report served",
		"user_id", userID,
		"from", from, "to", to,
		"rows", len(report.DailyReports),
		"requester", r.Header.Get("X-JWT-Sub"),
	)
}

// GET /reports/me/pdf?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Returns a CDN URL for the user's PDF report.
// Cache flow:
//   1. StatObject — if present in S3, return CDN URL immediately (no ClickHouse hit).
//   2. Cache miss — fetchReport (ClickHouse) → pdf-service/render → PutObject → return CDN URL.
//
// The CDN (Nginx) reverse-proxies MinIO and caches responses, so repeated
// downloads of the same URL don't reach MinIO either.
// Required role: prothetic_user OR administrator.
func (s *server) handleMyReportPDF(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-JWT-Sub")

	if !hasRole(r, "prothetic_user") && !hasRole(r, "administrator") {
		http.Error(w, "forbidden: prothetic_user or administrator role required", http.StatusForbidden)
		return
	}

	from, to, ok := parseDateRange(w, r)
	if !ok {
		return
	}

	// Cache key is stable for a given user + date range (historical data is immutable).
	cacheKey := fmt.Sprintf("reports/%s/%s_%s.pdf", userID, from, to)

	// S3+CDN path: check cache, generate on miss, always return a URL.
	if s.s3 != nil {
		cdnFileURL := strings.TrimRight(s.cfg.cdnURL, "/") + "/" + s.cfg.s3Bucket + "/" + cacheKey

		// Cache hit: object already in S3 — skip ClickHouse and pdf-service entirely.
		_, err := s.s3.StatObject(r.Context(), s.cfg.s3Bucket, cacheKey, minio.StatObjectOptions{})
		if err == nil {
			s.log.Info("pdf cache hit", "user_id", userID, "key", cacheKey)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"url": cdnFileURL}) //nolint:errcheck
			return
		}

		// Cache miss: generate PDF and upload synchronously before returning the URL.
		pdfBytes, genErr := s.generatePDF(r.Context(), userID, from, to)
		if genErr != nil {
			s.log.Error("generate pdf", "user_id", userID, "err", genErr)
			http.Error(w, genErr.Error(), http.StatusInternalServerError)
			return
		}

		filename := fmt.Sprintf("report_%s_%s.pdf", from, to)
		_, err = s.s3.PutObject(r.Context(), s.cfg.s3Bucket, cacheKey,
			bytes.NewReader(pdfBytes), int64(len(pdfBytes)),
			minio.PutObjectOptions{
				ContentType:        "application/pdf",
				ContentDisposition: `attachment; filename="` + filename + `"`,
			})
		if err != nil {
			s.log.Error("s3 upload failed", "key", cacheKey, "err", err)
			http.Error(w, "failed to store report", http.StatusInternalServerError)
			return
		}

		s.log.Info("pdf uploaded to s3", "user_id", userID, "key", cacheKey)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"url": cdnFileURL}) //nolint:errcheck
		return
	}

	// Fallback when S3 is not configured: stream the PDF directly.
	pdfBytes, err := s.generatePDF(r.Context(), userID, from, to)
	if err != nil {
		s.log.Error("generate pdf (no s3)", "user_id", userID, "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	filename := fmt.Sprintf("report_%s_%s.pdf", from, to)
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Write(pdfBytes) //nolint:errcheck
	s.log.Info("pdf streamed directly (no s3)", "user_id", userID, "from", from, "to", to)
}

// generatePDF fetches report data from ClickHouse and renders it via pdf-service.
func (s *server) generatePDF(ctx context.Context, userID, from, to string) ([]byte, error) {
	report, err := s.fetchReport(userID, from, to)
	if err != nil {
		return nil, fmt.Errorf("fetch report: %w", err)
	}

	jsonBody, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("encode report: %w", err)
	}

	pdfURL := strings.TrimRight(s.cfg.pdfServiceURL, "/") + "/render"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pdfURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("build pdf request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pdf service unavailable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("pdf generation failed (%d): %s", resp.StatusCode, body)
	}

	return io.ReadAll(resp.Body)
}

// GET /health — liveness probe for Docker / load balancers.
func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// ── Helpers ────────────────────────────────────────────────────────────────────

// parseDateRange reads optional ?from and ?to query params (YYYY-MM-DD).
// Defaults: from = 30 days ago, to = yesterday.
// Maximum allowed date is yesterday: Airflow processes data for the previous
// day only, so today and future dates are never present in ClickHouse.
// Returns (from, to, true) on success; writes a 400 and returns false on bad input.
func parseDateRange(w http.ResponseWriter, r *http.Request) (from, to string, ok bool) {
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	from = now.AddDate(0, 0, -30).Format("2006-01-02")
	to = yesterday

	if v := r.URL.Query().Get("from"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'from' must be YYYY-MM-DD", http.StatusBadRequest)
			return "", "", false
		}
		if v > yesterday {
			http.Error(w, fmt.Sprintf("'from' cannot exceed %s: Airflow processes data for the previous day only", yesterday), http.StatusBadRequest)
			return "", "", false
		}
		from = v
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'to' must be YYYY-MM-DD", http.StatusBadRequest)
			return "", "", false
		}
		if v > yesterday {
			http.Error(w, fmt.Sprintf("'to' cannot exceed %s: Airflow processes data for the previous day only", yesterday), http.StatusBadRequest)
			return "", "", false
		}
		to = v
	}
	return from, to, true
}

// buildReport assembles a UserReport from raw ClickHouse rows.
func buildReport(userID, from, to string, rows []chRow) *UserReport {
	report := &UserReport{
		UserID:       userID,
		Period:       Period{From: from, To: to},
		DailyReports: make([]DailyReport, 0, len(rows)),
	}

	// Profile fields are constant across rows for the same user_id.
	if len(rows) > 0 {
		r0 := rows[0]
		report.FirstName = r0.FirstName
		report.LastName = r0.LastName
		report.Email = r0.Email
		report.ProstheticsModel = r0.ProstheticsModel
		report.OrderDate = r0.OrderDate
		report.DeliveryDate = r0.DeliveryDate
		report.LastServiceDate = r0.LastServiceDate
	}

	var (
		sumSignal  float64
		sumBattery float64
		sumActive  uint32
		sumMove    uint32
		sumErr     uint32
		activeDays int
	)

	for _, row := range rows {
		report.DailyReports = append(report.DailyReports, DailyReport{
			Date:               row.ReportDate,
			TotalSessions:      row.TotalSessions,
			TotalActiveMinutes: row.TotalActiveMin,
			AvgSignalStrength:  row.AvgSignal,
			MaxSignalStrength:  row.MaxSignal,
			MovementCount:      row.MovementCount,
			ErrorCount:         row.ErrorCount,
			AvgBatteryLevel:    row.AvgBattery,
			MinBatteryLevel:    row.MinBattery,
		})
		sumActive += row.TotalActiveMin
		sumMove += row.MovementCount
		sumErr += row.ErrorCount
		sumSignal += float64(row.AvgSignal)
		sumBattery += float64(row.AvgBattery)
		if row.TotalSessions > 0 {
			activeDays++
		}
	}

	n := len(rows)
	report.Summary = Summary{
		TotalDaysActive:    activeDays,
		TotalActiveMinutes: sumActive,
		TotalMovements:     sumMove,
		TotalErrors:        sumErr,
		AvgSignalStrength:  round2(sumSignal, n),
		AvgBatteryLevel:    round2(sumBattery, n),
	}

	return report
}

func round2(sum float64, n int) float32 {
	if n == 0 {
		return 0
	}
	return float32(math.Round(sum/float64(n)*100) / 100)
}

// escapeString prevents SQL injection by escaping single-quote characters.
// user_id values come from JWT sub claims (Keycloak UUIDs), so in practice
// they never contain quotes; this is a defence-in-depth measure.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// ── CORS middleware ────────────────────────────────────────────────────────────

func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.cfg.frontendURL)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── main ───────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()
	srv := newServer(cfg)

	mux := http.NewServeMux()

	// Self-service JSON: user_id is taken from JWT sub, never from the URL.
	// Any authenticated user with prothetic_user or administrator role can call this.
	mux.HandleFunc("GET /reports/me", srv.requireAuth(srv.handleMyReport))

	// Self-service PDF: orchestrates ClickHouse → pdf-service → PDF stream.
	mux.HandleFunc("GET /reports/me/pdf", srv.requireAuth(srv.handleMyReportPDF))

	// Admin lookup: requires administrator role (enforced by requireAdmin middleware).
	// prothetic_user gets 403 before the handler runs, regardless of the user_id value.
	mux.HandleFunc("GET /reports/{user_id}", srv.requireAdmin(srv.handleAdminGetReport))

	// Liveness probe — no auth required.
	mux.HandleFunc("GET /health", srv.handleHealth)

	slog.Info("reports-api ready",
		"addr", cfg.listenAddr,
		"clickhouse", cfg.clickhouseURL,
	)

	if err := http.ListenAndServe(cfg.listenAddr, srv.cors(mux)); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
