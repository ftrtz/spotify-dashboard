FROM python:3.11-slim

WORKDIR /app

COPY /app .

# Install system dependencies for psycopg2
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev && \
    apt-get install -y iputils-ping && \
    rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

CMD ["streamlit", "run", "app.py"]
