FROM python:3.11-slim

WORKDIR /app

ARG CACHE_BUST=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["gunicorn", "-k", "eventlet", "-b", "0.0.0.0:8000", "nf-token-web:app"]