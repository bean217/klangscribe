import json
import urllib.request
from ray.job_submission import JobSubmissionClient
import dagster as dg


def test_ray_dashboard_localhost(url: str = "http://127.0.0.1:8265/api/version") -> None:
    with urllib.request.urlopen(url, timeout=5) as resp:
        body = resp.read()
    data = json.loads(body.decode("utf-8"))
    print("OK: Ray dashboard reachable:", data)


def test_submit_ray_job(address: str = "http://127.0.0.1:8265") -> None:
    client = JobSubmissionClient(address)

    job_id = client.submit_job(
        entrypoint='python -c "print(\'hello from ray job\')"'
    )
    print("submitted job_id:", job_id)

    status = client.get_job_status(job_id)
    print("status:", status)

    logs = client.get_job_logs(job_id)
    print("logs:\n", logs)

@dg.op
def test_ray():
    test_ray_dashboard_localhost()
    test_submit_ray_job()


@dg.job
def test_ray_job():
    test_ray()


@dg.op
def test_exec():
    print("Hello World!")


@dg.job
def test_exec_job():
    test_exec()
