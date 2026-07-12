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

Three layers, all containerised and deployable to Kubernetes. The platform runs two parallel
data streams: one for **real-time driver positions** (live map) and one for **trip lifecycle
events** (historical records and fares).

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
│  trip-producer        → @Scheduled trip lifecycle simulator        │
│  location-aggregator  → Kafka consumer → Redis live positions      │
│  trip-event-service   → consumes trip.events → Postgres            │
│  surge-pricing-engine → Kafka Streams windowed join                │
│  notification-service → Redis pub/sub → WebSocket relay            │
│  dlq-processor        → reads events.dlq → audit table             │
└───────────────────────────────┬───────────────────────────────────┘
                                 │  publish / consume
┌───────────────────────────────┴───────────────────────────────────┐
│  DATA LAYER    Apache Kafka · Redis · Postgres                     │
│  Kafka: 4 topics, partitioned by driverId / tripId / zoneId        │
│  Redis: live driver positions + pub/sub                            │
│  Postgres: trip history, fares, DLQ audit                          │
└─────────────────────────────────────────────────────────────────┘

INFRASTRUCTURE  Docker Compose (local) · Kubernetes + Helm (cloud)
OBSERVABILITY   Prometheus + Grafana
```

### Real-time location stream

Answers: *where are drivers right now?* Powers the live map.

```
gps-producer (50 drivers moving)
    ↓ produces every 2 seconds
driver.location topic (8 partitions, keyed by driverId)
    ↓ consumed by
location-aggregator
    ↓ writes to
Redis HASH (key: driver:{driverId}, value: {lat, lng, timestamp})
    ↓ publishes to
Redis pub/sub channel
    ↓ pushed via
notification-service (WebSocket)
    ↓ received by
React frontend → drivers move on the map in real time
```

### Trip lifecycle stream

Answers: *what trips happened, who rode with whom, and how much was paid?* Powers trip history.

```
trip-producer (simulates trip lifecycles)
    ↓ produces TRIP_STARTED / TRIP_ENDED / FARE_CALCULATED
trip.events topic (4 partitions, keyed by tripId)
    ↓ consumed by
trip-event-service
    ↓ writes to
Postgres (trips, fares tables)
    ↓ queried by
api-gateway → /api/trips endpoint → React shows trip history
```

### Data storage strategy


| Storage  | Purpose                   | Data                       | Lifespan                       |
| -------- | ------------------------- | -------------------------- | ------------------------------ |
| Redis    | Current state (real-time) | Driver locations (lat/lng) | Overwritten every ~2 seconds   |
| Postgres | Historical records        | Trips, fares, drivers      | Permanent (audit trail)          |
| Kafka    | Event stream (in-flight)  | Events being processed     | Retained 7 days (configurable) |


### Postgres entity model

```
Driver
├── driverId (PK)
├── name
├── vehicleType
└── HAS MANY Trips

Trip
├── tripId (PK)
├── driverId (FK → Driver)
├── riderId
├── pickupLat, pickupLng (captured at TRIP_STARTED)
├── dropoffLat, dropoffLng (captured at TRIP_ENDED)
├── distance (km)
├── duration (seconds)
├── status (STARTED, COMPLETED)
├── startTime
├── endTime
└── HAS ONE Fare

Fare
├── fareId (PK)
├── tripId (FK → Trip)
├── amount (calculated from distance + duration + surge)
├── surgeMultiplier
└── calculatedAt
```

### End-to-end user journey

A complete ride from live tracking through persistence and query:

1. **Driver is moving** — `gps-producer` emits a `DriverLocationEvent` (e.g. `driverId=D123`,
   `lat=40.7128`, `lng=-74.0060`) → `driver.location` topic → `location-aggregator` consumes →
   Redis: `driver:D123 = {lat: 40.7128, lng: -74.0060}` → frontend: driver D123 marker moves on
   the map.

2. **Rider requests a trip** — `trip-producer` emits
   `TripEvent(eventType=TRIP_STARTED, tripId=T456, driverId=D123, riderId=R789,
   pickupLat=40.7128, pickupLng=-74.0060)` → `trip.events` topic → `trip-event-service` consumes →
   Postgres: `INSERT INTO trips (tripId, driverId, riderId, pickupLat, pickupLng, status=STARTED)`.

3. **Driver completes the trip** — `trip-producer` emits
   `TripEvent(eventType=TRIP_ENDED, tripId=T456, dropoffLat=40.7580, dropoffLng=-73.9855,
   distance=5.2, duration=840)` → `trip.events` → `trip-event-service` → Postgres:
   `UPDATE trips SET dropoffLat=..., distance=5.2, duration=840, status=COMPLETED`.

4. **Fare is calculated** — `trip-producer` emits
   `TripEvent(eventType=FARE_CALCULATED, tripId=T456, fareAmount=18.50)` → `trip.events` →
   `trip-event-service` → Postgres: `INSERT INTO fares (tripId, amount=18.50)`.

5. **User views trip history** — frontend calls `GET /api/trips/T456` → `api-gateway` routes to
   `trip-event-service` → Postgres: `SELECT * FROM trips WHERE tripId=T456` → returns trip details
   including pickup, dropoff, distance, duration, and fare.

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
| `trip-producer`        | Kafka producer       | (scheduler)                          | `trip.events`                    |
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
mvn spring-boot:run -pl backend/trip-producer
#   a. gps-producer (port 8081)
#   b. location-aggregator (port 8082)
#   c. trip-producer (port 8084)
#   d. trip-event-service (port 8083)
#   e. surge-pricing-engine (port 8084)
#   f. notification-service (port 8085)
#   g. dlq-processor (port 8086)
#   h. api-gateway (port 8080)
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
│   ├── trip-producer/
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

