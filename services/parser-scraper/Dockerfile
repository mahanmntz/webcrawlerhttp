FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

CMD ["python", "parser_worker.py"]
