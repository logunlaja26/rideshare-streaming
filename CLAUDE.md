# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is a learning/portfolio project in active development. The ROADMAP.md tracks 10 phases (78 steps) — check it to understand what has been built vs. what is still planned.

## Commands

### Local development

```bash
# Start all infrastructure (Kafka KRaft, Redis, Postgres, Kafka UI, Prometheus, Grafana)
docker compose up -d

# Build all backend modules from the repo root
mvn clean install

# Run a single service
mvn spring-boot:run -pl backend/gps-producer
mvn spring-boot:run -pl backend/location-aggregator
mvn spring-boot:run -pl backend/trip-event-service
mvn spring-boot:run -pl backend/surge-pricing-engine
mvn spring-boot:run -pl backend/notification-service
mvn spring-boot:run -pl backend/dlq-processor
mvn spring-boot:run -pl backend/api-gateway

# Run tests for a single module
mvn test -pl backend/<module-name>

# Frontend
cd frontend/rideshare-ui && npm install && npm run dev   # → http://localhost:3000
```

### Infrastructure

```bash
# Provision AWS (EKS, RDS, ElastiCache, ECR, VPC) — takes ~12 min
cd infrastructure && terraform apply

# Tear down all AWS resources (drops cost from ~$5.35/day to ~$0.05/day)
# GitHub → Actions → "Destroy infrastructure" → Run workflow → type DESTROY
# OR locally:
cd infrastructure && terraform destroy

# Check outputs (EKS endpoint, ECR repo URLs, RDS host, app URL)
terraform output
```

### CI/CD

Pushing to `main` triggers the full pipeline: `ci.yml` (test + build + push images) → `deploy.yml` (terraform apply + kubectl rollout). PRs trigger CI + terraform plan as a PR comment.

## Architecture

Three-layer event-driven system. All services are containerised and Kubernetes-ready.

**Data flow:** `gps-producer` → `driver.location` Kafka topic (keyed by `driverId`, 8 partitions) → `location-aggregator` (writes to Redis HASH + publishes to Redis pub/sub) → `notification-service` (Redis pub/sub → STOMP WebSocket → browser).

**Kafka topics:**
- `driver.location` — 8 partitions, keyed by `driverId` (ordering guarantee per driver)
- `trip.events` — 4 partitions, keyed by `tripId`
- `surge.pricing` — 2 partitions, keyed by `zoneId`
- `events.dlq` — 1 partition (dead-lettered events from all consumers)

**Key design decisions to preserve:**
- All consumers use `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → `events.dlq`. Never swallow exceptions silently.
- Fare calculation path only uses exactly-once semantics (`transactional.id` + `@Transactional`). GPS/location updates use at-least-once (idempotent Redis writes).
- `location-aggregator` runs behind an HPA (2–8 replicas) — consumer group rebalancing is intentional behaviour, not a bug.
- Kafka runs as `bitnami/kafka` Helm chart on EKS (not MSK) to save ~$150/month.

**Backend modules** (Maven multi-module, parent POM at `backend/pom.xml`):
- `gps-producer` — `@Scheduled` random-walk simulator for 50 drivers, produces to `driver.location`
- `location-aggregator` — Kafka consumer with manual offset commit, writes Redis HASH + pub/sub
- `trip-event-service` — consumes `trip.events`, persists to Postgres via Spring Data JPA
- `surge-pricing-engine` — Kafka Streams windowed join (30s tumbling window, `driver.location` + `rider.requests`) → `surge.pricing`
- `notification-service` — Redis pub/sub subscriber → STOMP WebSocket relay (`/topic/drivers`)
- `dlq-processor` — consumes `events.dlq`, persists to Postgres `dlq_events` audit table
- `api-gateway` — Spring Cloud Gateway, routes `/api/drivers`, `/api/trips`, `/ws`

**Frontend** (`frontend/rideshare-ui`): React + Vite, react-leaflet, @stomp/stompjs, recharts, axios.

**Infrastructure** (`infrastructure/`): Terraform root + 6 modules (vpc, ecr, eks, rds, elasticache, iam). Terraform remote state in S3 + DynamoDB lock. GitHub Actions uses OIDC (`AWS_ROLE_ARN` secret only — no static credentials).

## Testing approach

- Unit tests use `@EmbeddedKafka` for producer/consumer logic
- Integration tests use Testcontainers (real Kafka + Postgres in Docker)
- Exactly-once correctness test: kill consumer mid-batch, verify zero duplicate fares in Postgres

## Local service endpoints

| Service         | URL                        |
|-----------------|----------------------------|
| Kafka UI        | http://localhost:8080      |
| Postgres        | localhost:5432             |
| Redis           | localhost:6379             |
| Prometheus      | http://localhost:9090      |
| Grafana         | http://localhost:3001      |
| Frontend        | http://localhost:3000      |
| Actuator/metrics| `http://localhost:<port>/actuator/prometheus` (per service) |
