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
// RBAC:
//   prothetic_user — may only fetch their own report (JWT sub == user_id)
//   administrator  — may fetch any user's report
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
	if token == authHeader { // no "Bearer " prefix
		return nil, fmt.Errorf("missing Bearer prefix")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed JWT: expected 3 parts, got %d", len(parts))
	}

	// JWT payload is the second segment (base64url, no padding).
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("base64 decode payload: %w", err)
	}

	var claims jwtClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal claims: %w", err)
	}
	if claims.Sub == "" {
		return nil, fmt.Errorf("JWT missing 'sub' claim")
	}
	return &claims, nil
}

// ── ClickHouse client ──────────────────────────────────────────────────────────

// chQuery sends SQL to ClickHouse via HTTP POST and returns the raw response.
// The query should end with "FORMAT JSONEachRow" for SELECT statements.
func chQuery(chURL, sql string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, chURL, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request: %w", err)
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
// ClickHouse JSONEachRow format. Field names must match column/alias names.
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
	Date              string  `json:"date"`
	TotalSessions     uint32  `json:"total_sessions"`
	TotalActiveMinutes uint32 `json:"total_active_minutes"`
	AvgSignalStrength float32 `json:"avg_signal_strength_mv"`
	MaxSignalStrength float32 `json:"max_signal_strength_mv"`
	MovementCount     uint32  `json:"movement_count"`
	ErrorCount        uint32  `json:"error_count"`
	AvgBatteryLevel   float32 `json:"avg_battery_level_pct"`
	MinBatteryLevel   float32 `json:"min_battery_level_pct"`
}

// Summary contains aggregated metrics across the entire requested period.
type Summary struct {
	TotalDaysActive   int     `json:"total_days_active"`
	TotalActiveMinutes uint32 `json:"total_active_minutes"`
	TotalMovements    uint32  `json:"total_movements"`
	TotalErrors       uint32  `json:"total_errors"`
	AvgSignalStrength float32 `json:"avg_signal_strength_mv"`
	AvgBatteryLevel   float32 `json:"avg_battery_level_pct"`
}

// Period holds the report's time window as ISO-8601 date strings.
type Period struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// UserReport is the top-level API response for GET /reports/{user_id}.
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

// ── Handlers ───────────────────────────────────────────────────────────────────

// GET /reports/{user_id}?from=YYYY-MM-DD&to=YYYY-MM-DD
//
// Returns the pre-computed prosthetics usage report for user_id.
// Data is read directly from the ClickHouse data mart — no real-time
// aggregation is performed at query time.
//
// Query parameters:
//
//	from  YYYY-MM-DD  start of period (default: 30 days ago)
//	to    YYYY-MM-DD  end of period   (default: yesterday)
func (s *server) handleGetReport(w http.ResponseWriter, r *http.Request) {
	// ── Step 1: authenticate ─────────────────────────────────────────────────
	claims, err := parseJWT(r.Header.Get("Authorization"))
	if err != nil {
		s.log.Warn("auth failed", "err", err, "ip", r.RemoteAddr)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	// ── Step 2: extract path variable {user_id} ──────────────────────────────
	targetUserID := r.PathValue("user_id")
	if targetUserID == "" {
		http.Error(w, "missing user_id path parameter", http.StatusBadRequest)
		return
	}

	// ── Step 3: RBAC ─────────────────────────────────────────────────────────
	isAdmin := claims.hasRole("administrator")
	isProthUser := claims.hasRole("prothetic_user")

	switch {
	case !isAdmin && !isProthUser:
		http.Error(w, "forbidden: role prothetic_user or administrator required", http.StatusForbidden)
		return
	case isProthUser && !isAdmin && claims.Sub != targetUserID:
		// prothetic_user may only see their own data
		s.log.Warn("RBAC: user_id mismatch",
			"sub", claims.Sub[:8], "requested", targetUserID[:min(8, len(targetUserID))])
		http.Error(w, "forbidden: you may only access your own report", http.StatusForbidden)
		return
	}

	// ── Step 4: parse date range ──────────────────────────────────────────────
	now := time.Now().UTC()
	fromDate := now.AddDate(0, 0, -30).Format("2006-01-02")
	toDate := now.AddDate(0, 0, -1).Format("2006-01-02")

	if v := r.URL.Query().Get("from"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'from' must be YYYY-MM-DD", http.StatusBadRequest)
			return
		}
		fromDate = v
	}
	if v := r.URL.Query().Get("to"); v != "" {
		if _, err := time.Parse("2006-01-02", v); err != nil {
			http.Error(w, "'to' must be YYYY-MM-DD", http.StatusBadRequest)
			return
		}
		toDate = v
	}

	// ── Step 5: query ClickHouse ──────────────────────────────────────────────
	//
	// FINAL forces ReplacingMergeTree deduplication so the client always gets
	// the latest version of each (user_id, report_date) row, even when
	// background merges haven't completed yet.
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
		escapeString(targetUserID),
		fromDate,
		toDate,
	)

	body, err := chQuery(s.cfg.clickhouseURL, sql)
	if err != nil {
		s.log.Error("clickhouse query failed", "user_id", targetUserID, "err", err)
		http.Error(w, "failed to fetch report data", http.StatusInternalServerError)
		return
	}

	// ── Step 6: parse JSONEachRow (one JSON object per newline) ──────────────
	var rows []chRow
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row chRow
		if err := json.Unmarshal(line, &row); err != nil {
			s.log.Error("parse clickhouse row", "err", err, "line", string(line))
			continue
		}
		rows = append(rows, row)
	}

	// ── Step 7: build and return response ────────────────────────────────────
	report := buildReport(targetUserID, fromDate, toDate, rows)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(report); err != nil {
		s.log.Error("encode response", "err", err)
	}

	s.log.Info("report served",
		"user_id", targetUserID,
		"from", fromDate, "to", toDate,
		"rows", len(rows),
		"requester", claims.Sub,
	)
}

// buildReport assembles a UserReport from raw ClickHouse rows.
func buildReport(userID, from, to string, rows []chRow) *UserReport {
	report := &UserReport{
		UserID:       userID,
		Period:       Period{From: from, To: to},
		DailyReports: make([]DailyReport, 0, len(rows)),
	}

	// Profile fields are constant across all rows for the same user_id;
	// take them from the first row.
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

	// Accumulate summary values while building the daily list.
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

// round2 computes the arithmetic mean of sum/n, rounded to 2 decimal places.
// Returns 0 when n == 0 to avoid division by zero.
func round2(sum float64, n int) float32 {
	if n == 0 {
		return 0
	}
	return float32(math.Round(sum/float64(n)*100) / 100)
}

// escapeString prevents SQL injection by escaping single-quote characters.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// min returns the smaller of two ints (backport for Go < 1.21 compat).
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GET /health
//
// Lightweight liveness probe used by Docker and load balancers.
func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
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

	// GET /reports/{user_id}          — full report (last 30 days default)
	// GET /reports/{user_id}?from=&to= — report for a custom date range
	mux.HandleFunc("GET /reports/{user_id}", srv.handleGetReport)

	// GET /health  — liveness probe
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
