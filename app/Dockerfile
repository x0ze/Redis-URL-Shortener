FROM python:3.13.7-alpine

WORKDIR /app
COPY requirements.txt .

RUN apk add --no-cache \
    python3-dev \
    build-base \
    mariadb-dev \
    openssl-dev \
    musl-dev \
    gcc \
    libc-dev \
    && pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 5000
CMD ["python", "app.py"]
