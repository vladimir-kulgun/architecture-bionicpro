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
)

// ── Configuration ──────────────────────────────────────────────────────────────

type config struct {
	listenAddr    string
	clickhouseURL string
	frontendURL   string
}

func loadConfig() config {
	return config{
		listenAddr:    ":" + getenv("PORT", "8000"),
		clickhouseURL: getenv("CLICKHOUSE_URL", "http://clickhouse:8123"),
		frontendURL:   getenv("FRONTEND_URL", "http://localhost:3000"),
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
}

func newServer(cfg config) *server {
	return &server{cfg: cfg, log: slog.Default()}
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

// serveReport is the shared implementation used by both handlers.
// It queries ClickHouse and writes the JSON response.
func (s *server) serveReport(w http.ResponseWriter, r *http.Request, userID, from, to string) {
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
		s.log.Error("clickhouse query failed", "user_id", userID, "err", err)
		http.Error(w, "failed to fetch report data", http.StatusInternalServerError)
		return
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

	report := buildReport(userID, from, to, rows)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		s.log.Error("encode response", "err", err)
	}

	s.log.Info("report served",
		"user_id", userID,
		"from", from, "to", to,
		"rows", len(rows),
		"requester", r.Header.Get("X-JWT-Sub"),
	)
}

// GET /health — liveness probe for Docker / load balancers.
func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
}

// ── Helpers ────────────────────────────────────────────────────────────────────

// parseDateRange reads optional ?from and ?to query params (YYYY-MM-DD).
// Defaults: from = 30 days ago, to = yesterday.
// Returns (from, to, true) on success; writes a 400 and returns false on bad input.
func parseDateRange(w http.ResponseWriter, r *http.Request) (from, to string, ok bool) {
	now := time.Now().UTC()
	from = now.AddDate(0, 0, -30).Format("2006-01-02")
	to = now.AddDate(0, 0, -1).Format("2006-01-02")

	if v := r.URL.Query().Get("from"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'from' must be YYYY-MM-DD", http.StatusBadRequest)
			return "", "", false
		}
		from = v
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'to' must be YYYY-MM-DD", http.StatusBadRequest)
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

	// Self-service: user_id is taken from JWT sub, never from the URL.
	// Any authenticated user with prothetic_user or administrator role can call this.
	mux.HandleFunc("GET /reports/me", srv.requireAuth(srv.handleMyReport))

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
