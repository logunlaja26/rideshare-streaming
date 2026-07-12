# Ride-Share Streaming — Build Roadmap

A 10-week, 10-phase plan to build the platform from scratch. Each phase lists concrete steps tagged
by discipline. The emphasis is on **backend / distributed systems** — the frontend is intentionally
thin, just enough to make the system visible and demo-able.

**Tags:** `[dist-sys]` distributed systems · `[backend]` · `[frontend]` · `[infra]` · `[ci/cd]`

**Progress overview:** 78 steps across 10 phases.

---

## Phase 1 — Environment setup (Week 1)

- [x] Install Java 21 (Temurin), Maven, Docker Desktop, kubectl, Helm, Terraform CLI, AWS CLI `[infra]`
- [x] Create GitHub repo `rideshare-streaming` with `/backend`, `/frontend`, `/infrastructure` folders `[ci/cd]`
- [x] Write `docker-compose.yml`: Kafka (KRaft mode), Redis, Postgres, Kafka UI on port 8080 `[infra]`
- [x] Run `docker compose up` — verify Kafka UI at localhost:8080, Postgres on 5432, Redis on 6379 `[infra]`
- [x] Create Spring Boot parent POM with dependency management: spring-kafka, JPA, Redis, Actuator, Lombok `[backend]`
- [x] Bootstrap 6 empty Spring Boot modules: gps-producer, location-aggregator, trip-event-service, surge-pricing-engine, notification-service, api-gateway `[backend]`
- [ ] Create S3 bucket + DynamoDB table for Terraform remote state (one-time manual AWS step) `[infra]` (deferred to deployment phase)
- [ ] Set up AWS OIDC identity provider + IAM role for GitHub Actions — no long-lived access keys `[ci/cd]` `[infra]` (deferred to deployment phase)

---

## Phase 2 — Kafka producer + core events (Week 2)

- [x] Define `DriverLocationEvent` Java record: driverId, lat, lng, timestamp, speed `[backend]`
- [x] Configure KafkaProducer: `acks=all`, `enable.idempotence=true`, JsonSerializer — key = driverId `[backend]` `[dist-sys]`
- [x] Write `@Scheduled` GPS simulator: random-walk algorithm, 50 simulated drivers, emit every 2 seconds `[backend]`
- [x] Create Kafka topics via AdminClient: driver.location (8 partitions), trip.events (4), surge.pricing (2), events.dlq (1) `[backend]` `[dist-sys]`
- [x] Understand WHY partitioning by driverId guarantees ordered delivery per driver — write it in your README `[dist-sys]`
- [x] Write unit tests for GPS simulator using `@EmbeddedKafka` broker `[backend]`
- [x] Verify events flowing in Kafka UI consumer view — confirm correct partitions `[backend]`

---

## Phase 3 — Consumer services (Week 3)

- [x] Build location-aggregator: `@KafkaListener` on driver.location, manual offset commit (`enable-auto-commit=false`), write to Redis HASH keyed by driverId `[backend]` `[dist-sys]`
- [x] Expose `GET /api/drivers` endpoint that reads all positions from Redis and returns GeoJSON `[backend]`
- [x] Build trip-producer: simulate trip lifecycle events (TRIP_STARTED, TRIP_ENDED, FARE_CALCULATED) and produce to trip.events topic `[backend]`
- [x] Build trip-event-service: consume trip.events, route TRIP_STARTED / TRIP_ENDED / FARE_CALCULATED via switch expression `[backend]`
- [ ] Persist completed trips to Postgres via Spring Data JPA — define Trip, Driver, Fare entities `[backend]`
- [ ] Implement DLQ on all consumers: `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` — failed messages → events.dlq `[backend]` `[dist-sys]`
- [ ] Understand and document: at-least-once (default) vs exactly-once delivery — tradeoffs in your README `[dist-sys]`
- [ ] Write integration tests using Testcontainers — real Kafka + Postgres running inside Docker in your tests `[backend]`

---

## Phase 4 — Kafka Streams + exactly-once (Weeks 4–5)

- [ ] Add kafka-streams dependency to surge-pricing-engine module `[backend]`
- [ ] Simulate rider.requests topic: emit demand events with (zone, riderCount) every 5 seconds `[backend]`
- [ ] Build KStream join: driver.location + rider.requests joined on zone with 30-second tumbling window `[backend]` `[dist-sys]`
- [ ] Compute demand/supply ratio per zone — emit SurgePricingEvent to surge.pricing when ratio > 1.5 `[backend]` `[dist-sys]`
- [ ] Understand windowed joins, why time skew matters in stream processing — add to README `[dist-sys]`
- [ ] Implement exactly-once for fare calc: set `transactional.id` on producer, `@Transactional` on consume-transform-produce loop `[backend]` `[dist-sys]`
- [ ] Test exactly-once: kill consumer mid-batch, verify zero duplicate fares in Postgres `[backend]` `[dist-sys]`
- [ ] Build DLQ processor service: read events.dlq topic, log failure reason, persist to dlq_events Postgres table for audit trail `[backend]`

---

## Phase 5 — WebSocket relay + observability (Week 6)

- [ ] Add spring-websocket + spring-messaging to notification-service `[backend]`
- [ ] Configure `@EnableWebSocketMessageBroker`: STOMP endpoint `/ws`, broker destination prefix `/topic` `[backend]`
- [ ] Subscribe notification-service to Redis pub/sub channel `driver-updates` using MessageListenerAdapter `[backend]`
- [ ] On each Redis message, call `SimpMessagingTemplate.convertAndSend("/topic/drivers", payload)` to push to browser subscribers `[backend]`
- [ ] Wire location-aggregator: after writing to Redis HASH, also publish to Redis pub/sub channel `driver-updates` `[backend]`
- [ ] Add Micrometer + Prometheus to all services: expose `/actuator/prometheus` endpoint on each pod `[backend]` `[dist-sys]`
- [ ] Add custom metrics: `rideshare.events.produced` counter, `rideshare.consumer.lag` gauge, `rideshare.dlq.total` counter `[backend]` `[dist-sys]`
- [ ] Add Prometheus + Grafana to docker-compose.yml — import Kafka consumer lag + JVM dashboard `[infra]`

---

## Phase 6 — API Gateway + React frontend (Week 7)

- [ ] Configure Spring Cloud Gateway: route /api/drivers → location-aggregator, /api/trips → trip-event-service, /ws → notification-service `[backend]`
- [ ] Add CORS config to gateway allowing React dev server at localhost:3000 `[backend]`
- [ ] Bootstrap React app: `npm create vite@latest rideshare-ui -- --template react` `[frontend]`
- [ ] Install dependencies: react-leaflet, @stomp/stompjs, sockjs-client, recharts, axios `[frontend]`
- [ ] Build LiveMap component: Leaflet map, useState for `{driverId: {lat,lng}}` driver positions, update markers on each WebSocket message `[frontend]`
- [ ] Connect STOMP WebSocket: subscribe to `/topic/drivers`, update driver state on every incoming frame `[frontend]`
- [ ] Build MetricsDashboard: poll `/actuator/metrics` every 5 seconds, display events/sec + consumer lag + DLQ count in Recharts line charts `[frontend]`
- [ ] Build SurgeHeatmap: receive surge.pricing events via WebSocket, colour map zones by multiplier (green → red) `[frontend]`

---

## Phase 7 — Dockerise all services (Week 8)

- [ ] Write multi-stage Dockerfile per Spring Boot service: `maven:3.9-eclipse-temurin-21` build stage + `eclipse-temurin:21-jre-alpine` runtime `[infra]`
- [ ] Write Dockerfile for React frontend: `node:20-alpine` build stage + `nginx:alpine` serve stage `[infra]` `[frontend]`
- [ ] Add all services to docker-compose.yml with health checks and `depends_on: [kafka, postgres, redis]` `[infra]`
- [ ] Test full local stack: `docker compose up` — all services start, map loads, drivers move on screen `[infra]`
- [ ] Write Kubernetes manifests: Deployment + Service + ConfigMap per microservice `[infra]`
- [ ] Add HorizontalPodAutoscaler for location-aggregator: minReplicas=2, maxReplicas=8 — observe Kafka consumer group rebalancing `[infra]` `[dist-sys]`
- [ ] Add PodDisruptionBudget: minAvailable=1 for all consumer services `[infra]` `[dist-sys]`
- [ ] Add liveness + readiness probes on all services pointing at `/actuator/health` `[infra]`

---

## Phase 8 — Terraform AWS infrastructure (Week 9)

- [ ] Write `terraform/modules/vpc`: VPC, 2 public + 2 private subnets, NAT gateway, route tables `[infra]`
- [ ] Write `terraform/modules/ecr`: one ECR repo per service (7 repos) — lifecycle policy to keep last 5 images `[infra]`
- [ ] Write `terraform/modules/eks`: EKS cluster, managed node group 2× t3.medium, OIDC provider for pod service accounts `[infra]`
- [ ] Write `terraform/modules/rds`: Postgres db.t3.micro, private subnet, security group allowing only EKS nodes `[infra]`
- [ ] Write `terraform/modules/elasticache`: Redis cache.t3.micro, private subnet `[infra]`
- [ ] Run Kafka on EKS as `bitnami/kafka` Helm chart — skip AWS MSK to save ~$150/month while developing `[infra]` `[dist-sys]`
- [ ] Run `terraform init` + `terraform plan` locally — review the execution plan carefully before applying `[infra]`
- [ ] Run `terraform apply` — cluster comes up (~12 min), verify EKS endpoint, RDS host in Terraform outputs `[infra]`

---

## Phase 9 — GitHub Actions CI/CD pipelines (Weeks 9–10)

- [ ] Write `.github/workflows/ci.yml`: trigger on push + PR, run `mvn test` across all backend modules in parallel `[ci/cd]`
- [ ] Add Docker build + ECR push job to ci.yml: runs only on push to main, image tagged with `$GITHUB_SHA` `[ci/cd]`
- [ ] Add terraform plan job to ci.yml: runs on PR, posts full plan diff as PR comment via `hashicorp/setup-terraform` action `[ci/cd]`
- [ ] Write `.github/workflows/deploy.yml`: triggers after CI passes on main — runs `terraform apply` then `kubectl rollout` update `[ci/cd]`
- [ ] Write `.github/workflows/destroy.yml`: workflow_dispatch only, requires typing DESTROY to confirm, runs `terraform destroy -auto-approve` `[ci/cd]`
- [ ] Add GitHub Actions Secrets: AWS_ROLE_ARN (OIDC role), TF_STATE_BUCKET, ECR_REGISTRY `[ci/cd]`
- [ ] Test full pipeline end-to-end: push a commit → CI passes → images built → deploy fires → verify on EKS `[ci/cd]`
- [ ] Test destroy workflow: trigger manually → verify all AWS resources gone → confirm daily cost drops to ~$0.05 `[ci/cd]` `[infra]`

---

## Phase 10 — Polish, documentation + portfolio (Week 10)

- [ ] Record a 60-second screen capture: drivers moving on map, surge zones lighting up, metrics dashboard updating live `[frontend]`
- [ ] Take Grafana screenshot: consumer lag graph during a simulated load spike — save for README + LinkedIn `[infra]`
- [ ] Write README section "Distributed systems decisions": exactly-once, partitioning strategy, DLQ, stream join, CAP tradeoffs in plain English `[dist-sys]`
- [ ] Write README section "Run in 3 commands": terraform apply → push triggers deploy → app live on EKS endpoint `[ci/cd]`
- [ ] Write README section "Tear down + cost": one GitHub Actions manual trigger → terraform destroy → $0.05/day `[ci/cd]`
- [ ] Add system architecture diagram to README — annotate each service with the distributed systems concept it demonstrates `[dist-sys]`
- [ ] Post on LinkedIn: GIF of live map + Grafana screenshot + repo link — #systemdesign #kafka #kubernetes #distributedsystems `[dist-sys]`
- [ ] Add to resume under Projects: real-time ride-share event streaming platform on AWS using Kafka, Kafka Streams, exactly-once semantics, Terraform IaC, OIDC-authenticated CI/CD `[dist-sys]`

---

## Suggested pacing

| Weeks | Phases | Focus                                  | Why it matters                                      |
|-------|--------|----------------------------------------|-----------------------------------------------------|
| 1–3   | 1–3    | Kafka working end-to-end               | Foundation — every later concept builds on this     |
| 4–6   | 4–5    | Streams, exactly-once, observability   | The depth that separates junior from senior         |
| 7     | 6      | Thin React frontend                    | Makes the system visible and demo-able              |
| 8–10  | 7–9    | Docker → Kubernetes → Terraform → CI/CD| Infrastructure as code, deploy/destroy automation   |
| 10    | 10     | README, GIF, LinkedIn, resume          | Converts a code project into a portfolio piece      |

> Don't rush phases 1–3. A working producer → consumer → Redis pipeline before week 4 is the
> milestone everything else depends on.
