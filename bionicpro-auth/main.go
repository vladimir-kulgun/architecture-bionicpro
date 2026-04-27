// bionicpro-auth — BFF (Backend for Frontend) authentication service.
//
// Responsibilities:
//   - Initiate PKCE Code Grant flow with Keycloak (code_verifier never leaves the server)
//   - Exchange authorisation code for tokens server-side
//   - Store access_token in memory; store refresh_token AES-256-GCM-encrypted in memory
//   - Bind both tokens to a session; issue an HTTP-only session cookie to the frontend
//   - Auto-refresh access_token via refresh_token when it expires
//   - Rotate session ID on every authenticated proxy request (session fixation prevention)
//   - Proxy authenticated requests to the upstream API with a Bearer token
//   - Fetch user profile from Keycloak UserInfo (populated from Yandex) and persist to PostgreSQL
package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Configuration ──────────────────────────────────────────────────────────────

type config struct {
	keycloakURL       string
	keycloakPublicURL string // browser-facing URL (may differ from internal)
	keycloakRealm     string
	clientID          string
	redirectURI       string
	frontendURL       string
	apiURL            string
	sessionLifetime   time.Duration
	cookieSecure      bool
	encryptionKey     string
	databaseURL       string
}

func loadConfig() config {
	lifetime := 1800
	fmt.Sscanf(getenv("SESSION_LIFETIME", "1800"), "%d", &lifetime)

	return config{
		keycloakURL:       getenv("KEYCLOAK_URL", "http://keycloak:8080"),
		keycloakPublicURL: getenv("KEYCLOAK_PUBLIC_URL", getenv("KEYCLOAK_URL", "http://keycloak:8080")),
		keycloakRealm:     getenv("KEYCLOAK_REALM", "reports-realm"),
		clientID:        getenv("KEYCLOAK_CLIENT_ID", "reports-frontend"),
		redirectURI:     getenv("AUTH_REDIRECT_URI", "http://localhost:8001/auth/callback"),
		frontendURL:     getenv("FRONTEND_URL", "http://localhost:3000"),
		apiURL:          getenv("API_URL", "http://api:8000"),
		sessionLifetime: time.Duration(lifetime) * time.Second,
		cookieSecure:    getenv("COOKIE_SECURE", "false") == "true",
		encryptionKey:   os.Getenv("ENCRYPTION_KEY"),
		databaseURL:     os.Getenv("DATABASE_URL"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── Crypto (AES-256-GCM) ───────────────────────────────────────────────────────

// deriveKey produces a 32-byte AES-256 key from an arbitrary secret via SHA-256.
func deriveKey(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// randomKey generates a cryptographically random 32-byte AES-256 key.
func randomKey() []byte {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		panic("rand.Reader: " + err.Error())
	}
	return key
}

// encryptGCM encrypts plaintext with AES-256-GCM; nonce is prepended to the result.
func encryptGCM(key []byte, plaintext string) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, []byte(plaintext), nil), nil
}

// decryptGCM is the inverse of encryptGCM.
func decryptGCM(key, data []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(data) < ns {
		return "", fmt.Errorf("ciphertext too short")
	}
	pt, err := gcm.Open(nil, data[:ns], data[ns:], nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}

// ── PKCE helpers ───────────────────────────────────────────────────────────────

// pkce returns a (code_verifier, code_challenge) pair for S256 PKCE.
func pkce() (verifier, challenge string) {
	b := make([]byte, 64)
	io.ReadFull(rand.Reader, b) //nolint:errcheck
	verifier = base64.RawURLEncoding.EncodeToString(b)
	h := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(h[:])
	return
}

// randomToken returns a URL-safe random string of n random bytes (base64-encoded).
func randomToken(n int) string {
	b := make([]byte, n)
	io.ReadFull(rand.Reader, b) //nolint:errcheck
	return base64.RawURLEncoding.EncodeToString(b)
}

// ── Domain types ────────────────────────────────────────────────────────────────

type session struct {
	accessToken      string
	encryptedRefresh []byte    // AES-256-GCM encrypted refresh_token
	expiresAt        time.Time
}

type pkceState struct {
	codeVerifier string
	createdAt    time.Time
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

// userInfoClaims holds the profile returned by Keycloak's UserInfo endpoint.
// Standard OIDC fields are populated by Keycloak attribute mappers from Yandex.
type userInfoClaims struct {
	Sub               string `json:"sub"`
	PreferredUsername string `json:"preferred_username"`
	Email             string `json:"email"`
	GivenName         string `json:"given_name"`
	FamilyName        string `json:"family_name"`
	YandexID          string `json:"yandex_id"` // mapped via IdP attribute mapper
}

// ── Database ───────────────────────────────────────────────────────────────────

const createTableSQL = `
CREATE TABLE IF NOT EXISTS user_profiles (
    id          BIGSERIAL    PRIMARY KEY,
    keycloak_id VARCHAR(255) UNIQUE NOT NULL,
    username    VARCHAR(255),
    email       VARCHAR(255),
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    yandex_id   VARCHAR(255),
    raw_claims  TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
)`

const upsertProfileSQL = `
INSERT INTO user_profiles
    (keycloak_id, username, email, first_name, last_name, yandex_id, raw_claims, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now())
ON CONFLICT (keycloak_id) DO UPDATE SET
    username   = EXCLUDED.username,
    email      = EXCLUDED.email,
    first_name = EXCLUDED.first_name,
    last_name  = EXCLUDED.last_name,
    yandex_id  = EXCLUDED.yandex_id,
    raw_claims = EXCLUDED.raw_claims,
    updated_at = now()`

func initDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	return pool, nil
}

// ── Server ─────────────────────────────────────────────────────────────────────

type server struct {
	cfg    config
	aesKey []byte
	log    *slog.Logger
	db     *pgxpool.Pool // nil when DATABASE_URL is not set

	sessionMu sync.RWMutex
	sessions  map[string]*session

	stateMu    sync.Mutex
	pkceStates map[string]*pkceState
}

func newServer(cfg config) *server {
	var key []byte
	if cfg.encryptionKey != "" {
		key = deriveKey(cfg.encryptionKey)
	} else {
		key = randomKey()
		slog.Warn("ENCRYPTION_KEY not set — using ephemeral key; sessions lost on restart")
	}

	srv := &server{
		cfg:        cfg,
		aesKey:     key,
		log:        slog.Default(),
		sessions:   make(map[string]*session),
		pkceStates: make(map[string]*pkceState),
	}

	if cfg.databaseURL != "" {
		pool, err := initDB(context.Background(), cfg.databaseURL)
		if err != nil {
			slog.Error("DB init failed — profile saving disabled", "err", err)
		} else {
			srv.db = pool
			slog.Info("DB connected — user profiles will be persisted")
		}
	} else {
		slog.Warn("DATABASE_URL not set — user profiles will not be persisted")
	}

	return srv
}

// ── Session management ──────────────────────────────────────────────────────────

func (s *server) createSession(accessToken, refreshToken string, expiresIn int) (string, error) {
	enc, err := encryptGCM(s.aesKey, refreshToken)
	if err != nil {
		return "", fmt.Errorf("encrypt refresh token: %w", err)
	}
	id := randomToken(32)
	s.sessionMu.Lock()
	s.sessions[id] = &session{
		accessToken:      accessToken,
		encryptedRefresh: enc,
		expiresAt:        time.Now().Add(time.Duration(expiresIn) * time.Second),
	}
	s.sessionMu.Unlock()
	s.log.Info("session created", "id", id[:8])
	return id, nil
}

// rotateSession moves session data to a fresh ID; caller must hold write lock.
func (s *server) rotateSession(oldID string) (string, bool) {
	sess, ok := s.sessions[oldID]
	if !ok {
		return "", false
	}
	delete(s.sessions, oldID)
	newID := randomToken(32)
	s.sessions[newID] = sess
	s.log.Info("session rotated", "old", oldID[:8], "new", newID[:8])
	return newID, true
}

func (s *server) setSessionCookie(w http.ResponseWriter, id string) {
	http.SetCookie(w, &http.Cookie{
		Name:     "session_id",
		Value:    id,
		Path:     "/",
		MaxAge:   int(s.cfg.sessionLifetime.Seconds()),
		HttpOnly: true,
		Secure:   s.cfg.cookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

// ── Keycloak calls ─────────────────────────────────────────────────────────────

func (s *server) tokenURL() string {
	return fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token",
		s.cfg.keycloakURL, s.cfg.keycloakRealm)
}

func (s *server) authURL() string {
	return fmt.Sprintf("%s/realms/%s/protocol/openid-connect/auth",
		s.cfg.keycloakURL, s.cfg.keycloakRealm)
}

func (s *server) authPublicURL() string {
	return fmt.Sprintf("%s/realms/%s/protocol/openid-connect/auth",
		s.cfg.keycloakPublicURL, s.cfg.keycloakRealm)
}

func (s *server) userInfoURL() string {
	return fmt.Sprintf("%s/realms/%s/protocol/openid-connect/userinfo",
		s.cfg.keycloakURL, s.cfg.keycloakRealm)
}

func (s *server) postToken(r *http.Request, vals url.Values) (*tokenResponse, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		s.tokenURL(), strings.NewReader(vals.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("keycloak returned %d: %s", resp.StatusCode, body)
	}

	var tr tokenResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("decode token response: %w", err)
	}
	return &tr, nil
}

func (s *server) exchangeCode(r *http.Request, code, verifier string) (*tokenResponse, error) {
	return s.postToken(r, url.Values{
		"grant_type":    {"authorization_code"},
		"client_id":     {s.cfg.clientID},
		"redirect_uri":  {s.cfg.redirectURI},
		"code":          {code},
		"code_verifier": {verifier},
	})
}

func (s *server) doRefresh(r *http.Request, refreshToken string) (*tokenResponse, error) {
	return s.postToken(r, url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {s.cfg.clientID},
		"refresh_token": {refreshToken},
	})
}

// fetchUserInfo calls Keycloak's UserInfo endpoint with the given access token.
// The returned claims include attributes mapped from Yandex by IdP mappers.
func (s *server) fetchUserInfo(ctx context.Context, accessToken string) ([]byte, *userInfoClaims, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.userInfoURL(), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("userinfo %d: %s", resp.StatusCode, raw)
	}

	var claims userInfoClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil, nil, fmt.Errorf("decode userinfo: %w", err)
	}
	return raw, &claims, nil
}

// saveProfile upserts the user's profile into the user_profiles table.
// Runs asynchronously so it never blocks the HTTP response.
func (s *server) saveProfile(accessToken string) {
	if s.db == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		raw, claims, err := s.fetchUserInfo(ctx, accessToken)
		if err != nil {
			s.log.Warn("fetchUserInfo failed — profile not saved", "err", err)
			return
		}

		_, err = s.db.Exec(ctx, upsertProfileSQL,
			claims.Sub,
			claims.PreferredUsername,
			claims.Email,
			claims.GivenName,
			claims.FamilyName,
			claims.YandexID,
			string(raw),
		)
		if err != nil {
			s.log.Error("upsert profile", "err", err)
			return
		}
		s.log.Info("profile saved", "sub", claims.Sub, "username", claims.PreferredUsername)
	}()
}

// ── HTTP handlers ──────────────────────────────────────────────────────────────

// GET /auth/login
//
// Generates a fresh PKCE pair and state token, stores them server-side, then
// redirects the browser to Keycloak's authorization endpoint. The code_verifier
// never leaves the server, satisfying the BFF security requirement.
func (s *server) handleLogin(w http.ResponseWriter, r *http.Request) {
	verifier, challenge := pkce()
	state := randomToken(16)

	s.stateMu.Lock()
	s.pkceStates[state] = &pkceState{codeVerifier: verifier, createdAt: time.Now()}
	s.stateMu.Unlock()

	params := url.Values{
		"response_type":         {"code"},
		"client_id":             {s.cfg.clientID},
		"redirect_uri":          {s.cfg.redirectURI},
		"scope":                 {"openid roles"},
		"state":                 {state},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
	}
	http.Redirect(w, r, s.authPublicURL()+"?"+params.Encode(), http.StatusFound)
}

// GET /auth/callback
//
// Keycloak redirects here after the user authenticates (+ consents + completes OTP).
// Validates state, exchanges the authorisation code for tokens using the stored
// code_verifier, creates an encrypted server-side session, issues an HTTP-only
// session cookie, and asynchronously persists the user profile to PostgreSQL.
func (s *server) handleCallback(w http.ResponseWriter, r *http.Request) {
	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state")
	if code == "" || state == "" {
		http.Error(w, "missing code or state", http.StatusBadRequest)
		return
	}

	s.stateMu.Lock()
	ps, ok := s.pkceStates[state]
	if ok {
		delete(s.pkceStates, state)
	}
	s.stateMu.Unlock()

	if !ok || time.Since(ps.createdAt) > 10*time.Minute {
		http.Error(w, "invalid or expired state", http.StatusBadRequest)
		return
	}

	tr, err := s.exchangeCode(r, code, ps.codeVerifier)
	if err != nil {
		s.log.Error("token exchange failed", "err", err)
		http.Error(w, "token exchange failed", http.StatusUnauthorized)
		return
	}

	sessionID, err := s.createSession(tr.AccessToken, tr.RefreshToken, tr.ExpiresIn)
	if err != nil {
		s.log.Error("create session", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Fetch user profile from Keycloak UserInfo (includes Yandex-sourced attributes)
	// and persist to DB asynchronously — does not block the redirect.
	s.saveProfile(tr.AccessToken)

	s.setSessionCookie(w, sessionID)
	http.Redirect(w, r, s.cfg.frontendURL, http.StatusFound)
}

// GET /auth/session
//
// Lightweight session check used by the frontend to determine auth state.
// Returns 200 {"status":"authenticated"} if a valid session cookie is present,
// 401 otherwise. Does not rotate the session or touch tokens.
func (s *server) handleSession(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("session_id")
	if err != nil {
		http.Error(w, "no session", http.StatusUnauthorized)
		return
	}
	s.sessionMu.RLock()
	_, ok := s.sessions[cookie.Value]
	s.sessionMu.RUnlock()

	if !ok {
		http.Error(w, "session not found", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "authenticated"}) //nolint:errcheck
}

// POST /auth/logout
//
// Destroys the server-side session and clears the cookie. Tokens are removed
// from memory; the frontend is effectively logged out immediately without
// needing to wait for token expiry.
func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if cookie, err := r.Cookie("session_id"); err == nil {
		s.sessionMu.Lock()
		delete(s.sessions, cookie.Value)
		s.sessionMu.Unlock()
		s.log.Info("session destroyed", "id", cookie.Value[:8])
	}
	http.SetCookie(w, &http.Cookie{Name: "session_id", Value: "", Path: "/", MaxAge: -1})
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "logged out"}) //nolint:errcheck
}

// ANY /api/{path...}
//
// Per-request flow:
//  1. Require valid session cookie.
//  2. If access_token is expired, refresh it via refresh_token (lock released during
//     network call to avoid holding the mutex over I/O).
//  3. Rotate session ID (new ID → old ID invalidated atomically).
//  4. Forward original request to the upstream API with a Bearer token.
//  5. Return upstream response + updated session cookie.
func (s *server) handleProxy(w http.ResponseWriter, r *http.Request) {
	
	cookie, err := r.Cookie("session_id")
	if err != nil {
		http.Error(w, "missing session cookie", http.StatusUnauthorized)
		return
	}
	sessionID := cookie.Value
	
	// ── Phase 1: read session under read lock ─────────────────────────────────
	s.sessionMu.RLock()
	sess, ok := s.sessions[sessionID]
	s.sessionMu.RUnlock()

	if !ok {
		http.Error(w, "session not found or expired", http.StatusUnauthorized)
		return
	}

	accessToken := sess.accessToken

	// ── Phase 2: refresh if expired (no lock held during network I/O) ────────
	if time.Now().After(sess.expiresAt) {
		s.log.Info("access token expired — refreshing", "session", sessionID[:8])

		s.sessionMu.RLock()
		encCopy := make([]byte, len(sess.encryptedRefresh))
		copy(encCopy, sess.encryptedRefresh)
		s.sessionMu.RUnlock()

		refreshToken, err := decryptGCM(s.aesKey, encCopy)
		if err != nil {
			s.sessionMu.Lock()
			delete(s.sessions, sessionID)
			s.sessionMu.Unlock()
			http.Error(w, "session corrupted", http.StatusUnauthorized)
			return
		}

		tr, err := s.doRefresh(r, refreshToken)
		if err != nil {
			s.sessionMu.Lock()
			delete(s.sessions, sessionID)
			s.sessionMu.Unlock()
			http.Error(w, "token refresh failed — please log in again", http.StatusUnauthorized)
			return
		}

		enc, err := encryptGCM(s.aesKey, tr.RefreshToken)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		s.sessionMu.Lock()
		if current, still := s.sessions[sessionID]; still {
			current.accessToken = tr.AccessToken
			current.encryptedRefresh = enc
			current.expiresAt = time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
			accessToken = current.accessToken
		} else {
			s.sessionMu.Unlock()
			http.Error(w, "session expired during refresh", http.StatusUnauthorized)
			return
		}
		s.sessionMu.Unlock()
	}

	// ── Phase 3: rotate session ID ────────────────────────────────────────────
	s.sessionMu.Lock()
	newSessionID, rotated := s.rotateSession(sessionID)
	s.sessionMu.Unlock()

	if !rotated {
		http.Error(w, "session expired", http.StatusUnauthorized)
		return
	}

	// ── Phase 4: forward to upstream API ─────────────────────────────────────
	// All /api/* requests go to reports-api (Go).
	// reports-api orchestrates internally: ClickHouse data → pdf-service render.
	upstreamPath := strings.TrimPrefix(r.URL.Path, "/api")
	upstreamURL := strings.TrimRight(s.cfg.apiURL, "/") + upstreamPath
	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	body, _ := io.ReadAll(r.Body)
	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, upstreamURL,
		bytes.NewReader(body))
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}

	skipHeaders := map[string]bool{
		"cookie": true, "authorization": true,
		"host": true, "content-length": true,
	}
	for k, vv := range r.Header {
		if !skipHeaders[strings.ToLower(k)] {
			for _, v := range vv {
				upstreamReq.Header.Add(k, v)
			}
		}
	}
	upstreamReq.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(upstreamReq)
	if err != nil {
		s.log.Error("upstream request failed", "err", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// ── Phase 5: relay response + rotated session cookie ──────────────────────
	// Skip CORS headers from upstream — the BFF's own cors() middleware owns them.
	skipResp := map[string]bool{
		"access-control-allow-origin":      true,
		"access-control-allow-credentials": true,
		"access-control-allow-methods":     true,
		"access-control-allow-headers":     true,
		"access-control-expose-headers":    true,
	}
	for k, vv := range resp.Header {
		if skipResp[strings.ToLower(k)] {
			continue
		}
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	s.setSessionCookie(w, newSessionID)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body) //nolint:errcheck
}

// ── CORS middleware ────────────────────────────────────────────────────────────

func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.cfg.frontendURL)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
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
	mux.HandleFunc("GET /auth/login", srv.handleLogin)
	mux.HandleFunc("GET /auth/callback", srv.handleCallback)
	mux.HandleFunc("GET /auth/session", srv.handleSession)
	mux.HandleFunc("POST /auth/logout", srv.handleLogout)
	mux.HandleFunc("/api/", srv.handleProxy)

	addr := ":8001"
	slog.Info("bionicpro-auth ready", "addr", addr,
		"keycloak", cfg.keycloakURL,
		"frontend", cfg.frontendURL,
		"db_enabled", cfg.databaseURL != "",
	)

	if err := http.ListenAndServe(addr, srv.cors(mux)); err != nil {
		slog.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
