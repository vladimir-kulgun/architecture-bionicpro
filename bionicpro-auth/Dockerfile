FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

# GONOSUMDB=* — skip checksum verification for go.sum generation
# -mod=mod    — allow Go to resolve and download dependencies at build time
ENV GONOSUMDB=*
RUN CGO_ENABLED=0 GOOS=linux go build -mod=mod -trimpath -o bionicpro-auth .

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/bionicpro-auth .

EXPOSE 8001
CMD ["./bionicpro-auth"]
