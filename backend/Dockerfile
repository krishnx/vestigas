# Use a lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirement files first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir gunicorn uvicorn

# Copy the rest of the backend source code
COPY . .

# Ensure Python can find the backend module
ENV PYTHONPATH=/app

# Expose port 80 (as expected by Traefik)
EXPOSE 80

# Default command for the container
CMD ["gunicorn", "main:app", "--workers", "1", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:80"]
