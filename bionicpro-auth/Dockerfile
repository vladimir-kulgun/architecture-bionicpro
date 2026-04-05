FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o bionicpro-auth .

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM alpine:3.19

# ca-certificates — needed for TLS calls to Keycloak
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bionicpro-auth .

EXPOSE 8001
CMD ["./bionicpro-auth"]
