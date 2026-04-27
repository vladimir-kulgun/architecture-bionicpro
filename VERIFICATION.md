# Верификация выполненных заданий

> Результаты каждого шага сохраняются в папке [VERIFICATIONS/](./VERIFICATIONS/).

## Фаза 0: Запуск системы

```bash
docker compose up -d
docker compose ps          # все контейнеры должны быть Running/healthy
docker compose logs --tail=50 bionicpro-auth reports-api kafka-connect airflow-webserver
```

**Ожидаемый результат:** все сервисы запущены без FATAL/ERROR.

---

## Задание 1: Безопасность

### Задача 2 — PKCE

```bash
grep -E "pkceCodeChallengeMethod|publicClient" keycloak/realm-export.json
# Ожидается: "pkceCodeChallengeMethod": "S256"
```

**Браузер:** открыть http://localhost:3000 → Login → URL редиректа содержит `code_challenge=` и `code_challenge_method=S256`.

### Задача 3 — BFF, сессионная cookie

```bash
grep -n "HttpOnly\|Secure\|SameSite" bionicpro-auth/main.go
grep -A2 "accessTokenLifespan" keycloak/realm-export.json   # <= 120s
```

**Браузер:** DevTools → Application → Cookies → cookie `session_id` с флагами HttpOnly; нет `access_token` в хранилище браузера.

**Тест авто-обновления:** подождать 2 минуты → выполнить запрос отчёта → должен вернуть данные (тихий refresh через refresh_token).

### Задача 4 — LDAP

**Keycloak Admin:** http://localhost:8080/admin → Realm: reports-realm → User Federation → запись `openldap` со статусом Sync OK.

### Задача 5 — MFA/OTP

**Браузер:** войти тестовым пользователем → система требует настройку OTP → отсканировать QR в Google Authenticator → ввести код → вход успешен. Повторный вход тоже требует OTP.

```bash
grep -i "CONFIGURE_TOTP" keycloak/realm-export.json
```

### Задача 6 — Yandex OAuth

```bash
grep -i "yandex\|identityProvider" keycloak/realm-export.json | head -5
```

**Браузер:** на странице входа кнопка «Войти через Яндекс» → редирект на Яндекс → после согласия возврат в приложение.

---

## Задание 2: Сервис отчётов

### Задача 2 — Airflow DAG

```bash
# Ручной запуск DAG
curl -X POST http://localhost:8082/api/v1/dags/bionicpro_etl/dagRuns \
  -H "Content-Type: application/json" -u admin:admin \
  -d '{"logical_date":"2025-01-01T00:00:00Z"}'
```

**Airflow UI:** http://localhost:8082 → DAG `bionicpro_etl` → все задачи зелёные.

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(*) FROM user_prosthetics_report"
# Ожидается: > 0
```

### Задача 3 — API отчётов

```bash
# Запрос с сессионной cookie (получить после входа в браузере)
curl "http://localhost:8001/api/reports/me?from=2025-01-01&to=2025-01-31" \
  --cookie "session_id=<ваш_session_id>"
# Ожидается: JSON с полями user_id, daily_reports
```

### Задача 4 — Ограничение доступа

```bash
# Без аутентификации → 401
curl -v http://localhost:8000/reports/me

# Доступ к данным другого пользователя → 403
curl -v "http://localhost:8001/api/reports/<чужой_user_id>" \
  --cookie "session_id=<ваша_сессия>"
```

### Задача 5 — Кнопка в UI

**Браузер:** http://localhost:3000 → войти → кнопка «Скачать отчёт (PDF)» видна → клик → браузер скачивает PDF или открывает CDN-ссылку.
DevTools → Network → запрос уходит на `/api/reports/me/pdf`.

---

## Задание 3: Снижение нагрузки на БД (S3/CDN)

```bash
# Первый запрос — промах кеша, PDF генерируется и загружается в MinIO
curl "http://localhost:8001/api/reports/me/pdf" --cookie "session_id=<сессия>"

# MinIO Console: http://localhost:9001 — убедиться, что PDF-объект появился в бакете

# Второй запрос — попадание в кеш (сравнить время ответа)
time curl "http://localhost:8001/api/reports/me/pdf" --cookie "session_id=<сессия>"

# CDN (Nginx) — заголовок X-Cache: HIT на повторном запросе
curl -I http://localhost:9080/reports/v1.pdf
```

```bash
cat nginx/nginx.conf   # proxy_cache_valid 365d для версионированных URL
```

---

## Задание 4: CDC / Debezium

```bash
# Статус коннектора Debezium
curl http://localhost:8083/connectors/crm-connector/status | python -m json.tool
# Ожидается: "state": "RUNNING"

# Данные в Kafka
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crm.public.crm_customers --from-beginning --max-messages 3

# Данные в ClickHouse
docker compose exec clickhouse clickhouse-client \
  --query "SELECT count(*) FROM raw_crm_customers"

# End-to-end тест CDC
docker compose exec crm_db psql -U crm_user crm_db \
  -c "INSERT INTO crm_customers (name, email) VALUES ('Test CDC','cdc@test.com');"
# Подождать 5–10 секунд:
docker compose exec clickhouse clickhouse-client \
  --query "SELECT * FROM raw_crm_customers WHERE email='cdc@test.com'"
```

---

## Чек-лист для PR

| Требование | Доказательство |
|---|---|
| Диаграмма архитектуры (Задания 1, 2) | Скриншот draw.io |
| PKCE (`code_challenge` в URL) | Скриншот DevTools Network |
| HTTP-only cookie, нет токенов в браузере | Скриншот DevTools → Application |
| Авто-обновление access_token | Лог запроса через 2+ мин |
| LDAP синхронизация | Скриншот Keycloak Admin |
| MFA обязательна | Скриншот OTP-запроса |
| Yandex OAuth | Скриншот редиректа на Яндекс |
| Airflow DAG — все задачи зелёные | Скриншот Airflow UI |
| Данные в ClickHouse | Вывод `SELECT count(*)` |
| JSON-отчёт по `/reports/me` | Вывод `curl` |
| 401 без аутентификации | Вывод `curl -v` |
| 403 на чужие данные | Вывод `curl -v` |
| PDF скачивается из UI | Скриншот браузера |
| CDN `X-Cache: HIT` | Вывод `curl -I` |
| Debezium RUNNING | JSON из `/connectors/status` |
| CDC end-to-end | Вывод `SELECT` до и после INSERT |
| Экспорт realm | Файл `keycloak/keycloak-results-export.json` |
