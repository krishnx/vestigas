# VESTIGAS – Backend Technical Assignment

## Background

At **VESTIGAS**, we digitize the construction supply chain by aggregating and verifying delivery data from suppliers, logistics providers, and construction sites.
This assignment simulates a core flow: **ingesting structured delivery data from multiple logistics providers**, transforming and validating it, storing it in an RDS, and exposing it in a unified format to downstream systems.

## Objective

Build a backend service that:

1.  Fetches delivery data from **two simulated logistics provider APIs**.
2.  Transforms and normalizes this data into a unified format.
3.  Stores the data in **relational persistent storage (RDS)**.
4.  Applies a filtering + scoring logic.
5.  Exposes a **job-based** REST API:
    -   Start a fetch job, get `jobId`.
    -   Check job status.
    -   Retrieve results (by `jobId` or via general query filters).

---

## Tech Requirements

We have provided a basic project scaffold to get you started, including a `docker-compose.yml` setup. However, you are free to modify it or use a different stack of technologies you are more familiar with. This includes your choice of build system (like Poetry or `uv`), database (Postgres, MySQL, or SQLite), and containerization tool (like Podman instead of Docker).

The firm requirements are:

-   **Language**: Python (version 3.11+ recommended, 3.10 is the minimum).
-   **Framework**: **FastAPI** is a must-have.
-   **Containerized**: The final submission must be runnable via a containerization tool.
-   **`start.sh` script**: The service must be startable with a `start.sh` script which handles the necessary environment variables:
    -   `HTTP_PORT`
    -   `LOGISTICS_A_URL`
    -   `LOGISTICS_B_URL`
    -   `DATABASE_URL` (e.g., `sqlite:///./vestigas.db`)
-   **Database**: Use a relational/SQL database.
-   **Environment variables**: Rename `.env.example` to `.env` to populate the values to `docker-compose`

---

## Mock Provider APIs

The `docker-compose.yml` orchestrates two mock API services to simulate the external logistics partners. They are accessible via a `traefik` reverse proxy.

Each mock service exposes its own interactive API documentation (Swagger UI) which can be used for testing and exploration:

-   **Partner A Swagger UI**: [`http://localhost:8000/mock-a/docs`](http://localhost:8000/mock-a/docs)
-   **Partner B Swagger UI**: [`http://localhost:8000/mock-b/docs`](http://localhost:8000/mock-b/docs)

### Partner A

-   **Route**: `http://localhost:8000/mock-a/api/logistics-a`
-   **Method**: `POST`
-   **Request Body**: None. The endpoint returns the full dataset regardless of any body sent.

**Response Body (200 OK)**

```json
[
  {
    "deliveryId": "DEL-001-A",
    "supplier": "SupplierX",
    "timestamp": "2025-08-01T07:34:00Z",
    "status": "delivered",
    "signedBy": "Martin Schulz"
  }
]
```

### Partner B

-   **Route**: `http://localhost:8000/mock-b/api/logistics-b`
-   **Method**: `POST`
-   **Request Body**: None. The endpoint returns the full dataset regardless of any body sent.

**Response Body (200 OK)**

```json
[
  {
    "id": "b-876543",
    "provider": "SupplierY",
    "deliveredAt": "2025-08-01T08:02:00+01:00",
    "statusCode": "OK",
    "receiver": {
      "name": "M. Schulz",
      "signed": true
    }
  }
]
```

---

## Unified Delivery Model

All delivery events must be transformed into:

```json
{
  "id": "DEL-001-A",
  "supplier": "SupplierX",
  "deliveredAt": "2025-08-01T07:34:00Z",
  "status": "delivered",
  "signed": true,
  "siteId": "munich-schwabing-1",
  "source": "Partner A",
  "deliveryScore": 1.2
}
```

### Transformation Rules

-   **Timestamps** standardized to **UTC ISO 8601**.
-   **Status codes** normalized:
    -   `"OK"` (from Partner B) → `"delivered"`
    -   `"FAILED"` → `"cancelled"`
    -   Any other status should be mapped to `"pending"`.
-   **Signature** normalized:
    -   `signed: true/false` based on presence of `signedBy` (Partner A) or `receiver.signed` (Partner B).
-   Add `source`: `"Partner A"` or `"Partner B"`.

### Scoring Logic

```
deliveryScore = (signed ? 1.0 : 0.3) × (isMorningDelivery ? 1.2 : 1.0)
```

-   Morning delivery: `deliveredAt` between `05:00` and `11:00` **UTC**.

---

## API – Job-Based Flow

The backend service is accessible via the reverse proxy at `http://localhost:8000/backend`.

***Note on Security:*** *For the purpose of this assignment, none of the API endpoints require authentication. In a production environment, these endpoints would be secured.*

### 1) Start Fetch Job

**POST** `/backend/deliveries/fetch`

Starts an async job that will call both partners, transform, score, and store results.

**Request**

```json
{
  "siteId": "munich-schwabing-1",
  "date": "2025-08-01"
}
```

**Response**

```json
{
  "jobId": "c9b0d4d1-5f6a-4c88-9a28-1d88d1b4a3f7",
  "status": "created"
}
```

---


### 2) Check Job Status

**GET** `/backend/deliveries/jobs/{jobId}`

**Response**

```json
{
  "jobId": "c9b0d4d1-5f6a-4c88-9a28-1d88d1b4a3f7",
  "status": "finished",
  "createdAt": "2025-08-01T06:00:00Z",
  "updatedAt": "2025-08-01T06:00:12Z",
  "input": { "siteId": "munich-schwabing-1", "date": "2025-08-01" },
  "stats": {
    "partnerA": { "fetched": 10, "transformed": 9, "errors": 1 },
    "partnerB": { "fetched": 7, "transformed": 7, "errors": 0 },
    "stored": 16
  },
  "error": null
}
```

---


### 3) Retrieve Results

**A) By jobId**
**GET** `/backend/deliveries/jobs/{jobId}/results`

Optional query params:
-   `limit`, `offset`
-   `supplier`
-   `status`
-   `signed`
-   `from`, `to`
-   `siteId`
-   `sortBy` (default: `deliveryScore desc`)

**Response**

```json
{
  "jobId": "c9b0d4d1-5f6a-4c88-9a28-1d88d1b4a3f7",
  "items": [
    {
      "id": "DEL-001-A",
      "supplier": "SupplierX",
      "deliveredAt": "2025-08-01T07:34:00Z",
      "status": "delivered",
      "signed": true,
      "siteId": "munich-schwabing-1",
      "source": "Partner A",
      "deliveryScore": 1.2
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

**B) All results**
**GET** `/backend/deliveries`

Same query params as above. Must default to sorting by `deliveryScore` desc.

---

## Processing Flow

1.  Create job with `status = created`.
2.  Background worker:
    -   Mark job `processing`.
    -   Call partners.
    -   Transform + score deliveries.
    -   Store results in DB.
    -   Mark job `finished` (or `failed` with error message).

---

## Error Handling & Resilience

The service must be robust against common issues with external APIs. A fetch job should not fail completely if only one of the two partners has issues; it should represent a partial success.

-   **Timeouts**: Implement network timeouts for all requests to the partner APIs (e.g., 5 seconds). If a partner API call times out, it should be treated as a failure for that specific partner. The job should still process data from the other partner if it was successful.

-   **Data Validation**: The data received from partner APIs must be validated. If an individual delivery record is malformed (e.g., a required field is missing, a timestamp is in the wrong format), that single record should be skipped. The error should be logged, and the job should continue processing other valid records from that partner. The `stats` object for the job should reflect the number of skipped/error records.

-   **API Errors & Downtime**: The service must gracefully handle cases where a partner API is unavailable (e.g., connection error) or returns an HTTP error status code (e.g., `500 Internal Server Error`, `503 Service Unavailable`). Such failures should be logged, and the job should proceed with data from the other partner if available.

-   **Partial Success**: A job should be marked as `finished` even if only one of the two partners provided data successfully. The `stats` object in the job status response must clearly reflect the outcome for each partner (fetched, transformed, errors). A job should only be marked as `failed` if *both* partners fail and no data could be processed at all.

-   **Idempotency**: When a new fetch job is requested for a `(siteId, date)` pair:
    -   If a job for that pair already exists with a status of `created` or `processing`, return the existing `jobId` with a `202 Accepted` status code.
    -   If a job for that pair already exists with a status of `finished` or `failed`, a new job should be created.

---

## Must-Haves

-   Modular, clean codebase.
-   External API integrations.
-   Data transformation layer.
-   Scoring logic.
-   **RDS persistence** (Postgres/MySQL/SQLite).
-   Unit tests: focus on transformation, status mapping, time normalization, scoring, filtering/sorting.
-   Error handling: timeouts, invalid responses, downtime.
-   Proper documentation

---

## Bonus

-   **Scheduled Fetching**: Implement a mechanism to automatically trigger the data fetching process on a schedule (e.g., a cron job that runs hourly) for a predefined list of sites, rather than relying solely on the API endpoint.
-   **Caching**: Implement a caching layer (e.g., in-memory or Redis) for the `GET /deliveries` and `GET /deliveries/jobs/{jobId}/results` endpoints to reduce database load and improve response times for frequent requests.
-   **Retry Strategy**: For transient network errors or `5xx` status codes from partner APIs, implement a robust retry mechanism with exponential backoff and jitter.
-   **Advanced Async Job Runner**: Use a dedicated job queue library (e.g., Celery, ARQ) for a more robust implementation of the asynchronous background worker.

---

## Submission

Provide a GitHub/GitLab/Bitbucket repo or `.zip` with:

-   Source code
-   Unit tests
-   `README.md` including:
    -   How to run the service
    -   Architecture overview
    -   Transformation & scoring description
    -   Example requests/responses
    -   Notes on retries, timeouts, caching, idempotency
    -   Potential evolution of the service

---

## Example cURL

Start job:

```bash
curl -X POST http://localhost:8000/backend/deliveries/fetch \
  -H "Content-Type: application/json" \
  -d '{"siteId":"munich-schwabing-1","date":"2025-08-01"}'
```

Check status:

```bash
curl http://localhost:8000/backend/deliveries/jobs/<jobId>
```

Get results by job:

```bash
curl "http://localhost:8000/backend/deliveries/jobs/<jobId>/results?limit=50&offset=0"
```

Query all results:

```bash
curl "http://localhost:8000/backend/deliveries?siteId=munich-schwabing-1&status=delivered"
```


```