# Ride-Share Streaming — Real-Time Distributed Event Platform

A production-style, real-time ride-share event streaming platform built to learn and demonstrate
distributed systems and system design principles. Simulated drivers emit GPS events, Kafka streams
them through a set of Spring Boot microservices, and a React + Leaflet frontend shows drivers moving
on a live map with surge pricing and live metrics.

**Stack:** Java 21 · Spring Boot · Apache Kafka · Kafka Streams · Redis · Postgres · Docker ·
Kubernetes · Terraform · GitHub Actions · React + Leaflet

---

## Why this project exists

This is a exploration + portfolio project. The goal is to practicalize system design principles and apply them to a real distributed systems work:

- Event-driven architecture with a message broker
- Partitioning strategy and ordered delivery guarantees
- Delivery semantics: at-least-once vs exactly-once (fare calculation)
- Dead letter queues and retry handling
- Stream processing with windowed joins
- Horizontal scaling and consumer group rebalancing (location-aggregator)
- Observability (metrics, consumer lag, dashboards)
- Infrastructure as code and one-command deploy / teardown (Terraform)

---

## Architecture

Three layers, all containerised and deployable to Kubernetes.

```
┌─────────────────────────────────────────────────────────────────┐
│  FRONTEND   React + Leaflet + STOMP WebSocket client              │
│  Live map · Metrics dashboard · Trip feed · Surge heatmap         │
└───────────────────────────────┬───────────────────────────────────┘
                                 │  WebSocket / REST
┌───────────────────────────────┴───────────────────────────────────┐
│  BACKEND    Java + Spring Boot microservices                       │
│                                                                    │
│  api-gateway          → Spring Cloud Gateway (routing + CORS)      │
│  gps-producer         → @Scheduled GPS simulator → Kafka producer  │
│  location-aggregator  → Kafka consumer → Redis live positions      │
│  trip-event-service   → consumes trip.events → Postgres            │
│  surge-pricing-engine → Kafka Streams windowed join                │
│  notification-service → Redis pub/sub → WebSocket relay            │
│  dlq-processor        → reads events.dlq → audit table             │
└───────────────────────────────┬───────────────────────────────────┘
                                 │  publish / consume
┌───────────────────────────────┴───────────────────────────────────┐
│  DATA LAYER    Apache Kafka · Redis · Postgres                     │
│  Kafka: 4 topics, partitioned by driverId                          │
│  Redis: live driver positions + pub/sub                            │
│  Postgres: trip history, fares, DLQ audit                          │
└─────────────────────────────────────────────────────────────────┘

INFRASTRUCTURE  Docker Compose (local) · Kubernetes + Helm (cloud)
OBSERVABILITY   Prometheus + Grafana
```

---

## Kafka topics


| Topic             | Partitions | Key        | Purpose                                     |
| ----------------- | ---------- | ---------- | ------------------------------------------- |
| `driver.location` | 8          | `driverId` | Raw GPS events from the producer            |
| `trip.events`     | 4          | `tripId`   | TRIP_STARTED / TRIP_ENDED / FARE_CALCULATED |
| `surge.pricing`   | 2          | `zoneId`   | Surge multipliers emitted per zone          |
| `events.dlq`      | 1          | —          | Dead-lettered / malformed events            |


Partitioning by `driverId` on `driver.location` is what guarantees ordered delivery of GPS
updates per driver — all events for one driver always land on the same partition, and a partition
is consumed in order by exactly one consumer in the group.

---

## Services


| Service                | Type                 | Reads from                           | Writes to                        |
| ---------------------- | -------------------- | ------------------------------------ | -------------------------------- |
| `gps-producer`         | Kafka producer       | (scheduler)                          | `driver.location`                |
| `location-aggregator`  | Kafka consumer       | `driver.location`                    | Redis HASH + pub/sub             |
| `trip-event-service`   | Kafka consumer       | `trip.events`                        | Postgres                         |
| `surge-pricing-engine` | Kafka Streams        | `driver.location` + `rider.requests` | `surge.pricing`                  |
| `notification-service` | WebSocket relay      | Redis pub/sub                        | Browser (STOMP `/topic/drivers`) |
| `dlq-processor`        | Kafka consumer       | `events.dlq`                         | Postgres `dlq_events`            |
| `api-gateway`          | Spring Cloud Gateway | (routes)                             | downstream services              |


---

## Running locally

Prerequisites: Java 21 (Temurin), Maven, Docker Desktop.

```bash
# 1. Start infrastructure (Kafka in KRaft mode, Redis, Postgres, Kafka UI)
docker compose up -d

# 2. Confirm infrastructure is up
#    Kafka UI   → http://localhost:8080
#    Postgres   → localhost:5432
#    Redis      → localhost:6379
#    Prometheus → http://localhost:9090
#    Grafana    → http://localhost:3001

# 3. Build all backend modules
mvn clean install

# 4. Start each service (separate terminals, or use docker compose for everything)
mvn spring-boot:run -pl backend/gps-producer
mvn spring-boot:run -pl backend/location-aggregator
#   a. gps-producer (port 8081)
#   b. location-aggregator (port 8082)
#   c. trip-event-service (port 8083)
#   d. surge-pricing-engine (port 8084)
#   e. notification-service (port 8085)
#   f. dlq-processor (port 8086)
#   g. api-gateway (port 8080)
# ...etc

# 5. Start the frontend
cd frontend/rideshare-ui
npm install
npm run dev          # → http://localhost:3000

# 6. Shutdown infrastructure when done
docker compose down
docker compose down -v # completely remove everything including volumes (fresh start)
```

You should see drivers begin moving on the map within a few seconds.

---

## Deploy to AWS (3 commands)

Infrastructure is fully managed by Terraform and deployed via GitHub Actions.

```bash
# 1. Provision AWS infrastructure (EKS, RDS, ElastiCache, ECR, VPC)
cd infrastructure && terraform apply

# 2. Push to main — CI builds images, pushes to ECR, deploy workflow rolls them out
git push origin main

# 3. App is live on the EKS load balancer endpoint (see terraform output)
terraform output app_url
```

See `TERRAFORM_STRUCTURE.md` for the full infrastructure layout.

---

## Tear down (cost control)

This is the cost kill-switch. One manual trigger destroys every AWS resource.

```
GitHub → Actions → "Destroy infrastructure" → Run workflow → type DESTROY
```

This runs `terraform destroy -auto-approve` and drops daily cost from ~$5/day to ~$0.05/day
(only ECR image storage remains).


| Resource              | Active cost/day | After destroy  |
| --------------------- | --------------- | -------------- |
| EKS control plane     | ~$2.40          | $0             |
| 2× t3.medium nodes    | ~$2.00          | $0             |
| RDS db.t3.micro       | ~$0.50          | $0             |
| ElastiCache t3.micro  | ~$0.40          | $0             |
| Kafka on EKS (no MSK) | $0 extra        | $0             |
| ECR storage           | ~$0.05          | ~$0.05         |
| **Total**             | **~$5.35/day**  | **~$0.05/day** |


> Running Kafka as a Helm chart on EKS instead of AWS MSK saves roughly $150/month during development.

---

## Distributed systems decisions

This section is the heart of the project — it explains *why* each choice was made.

### Partitioning strategy

**Why partitioning by `driverId` guarantees ordering:**

Kafka only guarantees ordering within a partition, not across partitions. By keying messages by `driverId`:

1. **Hash-based partition assignment** — Kafka's default partitioner computes `hash(driverId) % numPartitions`, ensuring all messages for the same driver always go to the same partition
2. **Single-consumer-per-partition** — Within a consumer group, each partition is assigned to exactly one consumer
3. **Sequential consumption** — A consumer reads messages from a partition in the order they were written

**Result:** All location updates for `driver-042` land on the same partition (e.g., partition 3) and are consumed in order, even if `driver-001`'s updates are processed in parallel on partition 0.

**Implementation details:**

```java
// gps-producer sends with driverId as the key
kafkaTemplate.send("driver.location", driverId, event);
```

The 8-partition count on `driver.location` allows up to 8 consumers to process in parallel while maintaining per-driver ordering. With 50 simulated drivers, each partition handles ~6-7 drivers.

**Topics and their partitioning keys:**

- `driver.location` (8 partitions, key: `driverId`) — GPS location updates
- `trip.events` (4 partitions, key: `tripId`) — Trip lifecycle events (ordered per trip)
- `surge.pricing` (2 partitions, key: `zoneId`) — Surge multipliers (ordered per zone)
- `events.dlq` (1 partition, no key) — Dead-lettered events (order not required)

### At-least-once vs exactly-once

Most consumers use **at-least-once** (the Kafka default) with idempotent writes — re-processing a GPS
update just overwrites the same Redis key, so duplicates are harmless. The **fare calculation** path
uses **exactly-once** semantics (`transactional.id` on the producer + `@Transactional` around the
consume-transform-produce loop) because charging a rider twice is unacceptable. Exactly-once costs
latency (~50ms vs ~18ms), which is a deliberate tradeoff applied only where correctness demands it.

### Dead letter queue

Every consumer is configured with `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`. Malformed
events (null coordinates, bad timestamps, deserialization failures) are routed to `events.dlq` instead
of crashing the consumer or blocking the partition. The `dlq-processor` persists them to an audit
table so failures are observable, not silent.

### Stream join (surge pricing)

The surge engine uses Kafka Streams to join `driver.location` (supply) against `rider.requests`
(demand) over a 30-second window, grouped by zone. When demand/supply exceeds 1.5 it emits a surge
event. This demonstrates windowed stream joins and why event-time vs processing-time matters.

### Scaling and availability

The `location-aggregator` runs behind a HorizontalPodAutoscaler (2–8 replicas). Scaling triggers
Kafka consumer group rebalancing, which redistributes partitions across the new pods. A
PodDisruptionBudget (`minAvailable: 1`) ensures rolling updates never take all consumers down at once.

### CAP tradeoff

The live map favours availability and partition tolerance over strict consistency — a driver position
that's a second stale is fine. The fare/payment path favours consistency — it must be correct even if
that means higher latency.

---

## Observability

- Micrometer exposes `/actuator/prometheus` on every service
- Custom metrics: `rideshare.events.produced` (counter), `rideshare.consumer.lag` (gauge),
`rideshare.dlq.total` (counter)
- Prometheus scrapes all pods; Grafana dashboards show consumer lag, throughput, and JVM health
- The consumer-lag-under-load graph is the single best screenshot for demonstrating the system holds
up under a simulated spike

---

## Repository layout

```
rideshare-streaming/
├── docker-compose.yml          # Kafka, Redis, Postgres, Grafana, Prometheus
├── README.md
├── ROADMAP.md                  # week-by-week build plan
├── TERRAFORM_STRUCTURE.md      # infrastructure layout
├── .github/workflows/          # ci.yml, deploy.yml, destroy.yml
├── backend/
│   ├── pom.xml                 # parent POM, dependency management
│   ├── gps-producer/
│   ├── location-aggregator/
│   ├── trip-event-service/
│   ├── surge-pricing-engine/
│   ├── notification-service/
│   ├── dlq-processor/
│   └── api-gateway/
├── frontend/
│   └── rideshare-ui/           # React + Leaflet + Recharts
├── k8s/                        # Helm chart or raw manifests
└── infrastructure/             # Terraform root + modules
```

---

