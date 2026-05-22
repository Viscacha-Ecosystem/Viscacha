# Viscacha

Run Python functions as background jobs — no Redis, no Celery, no broker of any kind. State lives in a local file. Jobs survive crashes.

```bash
pip install viscacha
```

---

## The problem it solves

You have a function that's slow, might fail, or needs to run outside your main process. You want to retry it automatically if it fails, and you don't want to lose it if your process dies.

The standard answer is Celery. Celery requires Redis or RabbitMQ, a separate worker process, and about an hour of setup before you write a single line of your actual code.

Viscacha is the other answer.

---

## Quickstart

```python
from viscacha import Client, Worker

client = Client()          # jobs live in memory
worker = Worker(client)

@worker.job("process", max_retries=3)
def process(file: str) -> dict:
    # do something slow or fallible
    return {"lines": count_lines(file)}

worker.run(blocking=False)

handle = client.enqueue("process", file="data.csv")
result = handle.wait(timeout=60)
print(result.result)   # {"lines": 4821}
print(result.status)   # "done"
```

That's it. No config files, no server to start, no dependencies to install.

---

## Jobs survive crashes

```python
# Add a file path to persist jobs across restarts
client = Client(log_path="jobs.db")
```

If your process crashes mid-job, the job goes back to the queue when you restart. Nothing is lost.

---

## Retries

```python
@worker.job("call_api", max_retries=5, lease_ttl=60.0)
def call_api(endpoint: str) -> dict:
    resp = requests.get(endpoint, timeout=10)
    resp.raise_for_status()
    return resp.json()
```

Any exception triggers a retry. After `max_retries` failures the job is marked permanently failed and you can inspect what went wrong. `lease_ttl` is how long a worker can hold a job before it's considered stalled and returned to the queue.

---

## Checking job status

```python
handle = client.enqueue("call_api", endpoint="https://api.example.com/data")

result = handle.wait(timeout=30)   # block until done, raises TimeoutError
result.status    # "done" | "failed" | "cancelled"
result.result    # return value of the function
result.error     # exception message if failed

handle.cancel()  # cancel while still pending

client.jobs()               # all jobs
client.jobs(status="done")  # filter by status
client.get(handle.id)       # get one by ID
```

---

## AI pipelines

Long-running LLM calls are a natural fit. Each call is a job — retries handle rate limits and transient errors, and you can run workers in parallel.

```python
import anthropic
from viscacha import Client, Worker

client = Client(log_path="jobs.db")
worker = Worker(client)
ai = anthropic.Anthropic()

@worker.job("classify", max_retries=3)
def classify(title: str, body: str) -> dict:
    msg = ai.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        messages=[{"role": "user", "content": f"Classify this ticket: {title}\n{body}"}],
    )
    return {"category": msg.content[0].text}

worker.run(blocking=False)

handles = [client.enqueue("classify", title=t, body=b) for t, b in tickets]
results = [h.wait(timeout=60) for h in handles]
```

---

## Scale to multiple machines

When you outgrow a single process, swap in the [Rust server](https://github.com/Viscacha-Ecosystem/Viscacha-rs) as a drop-in backend. One line change:

```python
# Before: in-process
client = Client(log_path="jobs.db")

# After: workers can run on any machine
client = Client(url="http://your-server:8000")
```

The Rust server adds a time-travel debugger UI, Prometheus metrics, Grafana dashboards, and a full HTTP API. Workers on separate machines, persistence across restarts, all the same Python code.

---

## Tests
pytest tests/
```

Python 3.10+
