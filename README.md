# VESTIGAS Backend Delivery Ingestion Service

This project implements a modular, fault-tolerant backend service using FastAPI to aggregate, transform, score, and store delivery data from multiple external logistics partners.

## 🚀 How to Run the Service

The entire application is containerized and orchestrated using `docker-compose`.

### Prerequisites
- Docker and Docker Compose installed.

### Execution
1.  **Build and Start:** Run the following command from the root directory:
    ```bash
    docker-compose up --build
    ```
2.  **Access:** The main service, mock APIs, and documentation will be accessible via the Traefik proxy on port `8000`.
    -   **API Base URL:** `http://localhost:8000/backend/deliveries`
    -   **Swagger UI (Docs):** `http://localhost:8000/backend/deliveries/docs`
    -   **Traefik Dashboard:** `http://localhost:8000/api/dashboard` (Shows service health)

### Running Tests
To run the unit and integration tests inside the Docker container:

```bash
docker-compose run backend pytest
```

### Make commands
1. Start your backend:
```bash
make up
```

2. Check health:
```bash
make health
```

3. Start a fetch job:
```bash
make fetch-job
```

4. Check job status (replace JOB_ID with actual ID returned from fetch-job):
```bash
make job-status JOB_ID=<job_id> 
```

5. Get job results:
```bash
make job-results JOB_ID=<job_id>
```

6. Search deliveries with filters:
```bash
make search-deliveries
```

7. Run all tests
```bash
make test
```

8. Format the code
```bash
make format
```

