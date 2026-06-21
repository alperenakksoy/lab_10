FROM python:3.12-slim

WORKDIR /app

RUN pip install flask requests matplotlib --no-cache-dir

# Task 3 + 4
COPY raft.py              .
COPY app.py               .
COPY dashboard.py         .
COPY raft_client.py       .
COPY measure_failover.py  .

# Task 5
COPY offline_app.py       .
COPY crdt_counter.py      .
COPY crdt_pn_counter.py   .
COPY crdt_or_set.py       .

CMD ["python", "app.py"]