FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY main.go ./

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o reports-api .

# ── Runtime image ──────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN sed -i 's|dl-cdn.alpinelinux.org/alpine|mirror.yandex.ru/mirrors/alpine|g' /etc/apk/repositories \
    && apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/reports-api .

EXPOSE 8000
CMD ["./reports-api"]
