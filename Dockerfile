# Use a builder stage to install dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy application files and install Python dependencies
COPY ./requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    apt-get install -y iputils-ping && \
    rm -rf /var/lib/apt/lists/*

# Copy only the necessary files from the builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY /app /app

CMD ["streamlit", "run", "app.py"]
