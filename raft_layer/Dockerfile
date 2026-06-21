# Dockerfile — Raft node, offline client, and dashboard (Task 4 + 5)
FROM python:3.12-slim

WORKDIR /app

RUN pip install flask requests matplotlib --no-cache-dir

# Task 3 + 4
COPY raft.py              .
COPY app.py               .
COPY dashboard.py         .
COPY raft_client.py       .
COPY measure_failover.py  .

CMD ["python", "app.py"]