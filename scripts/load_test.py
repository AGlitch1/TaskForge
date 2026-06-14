import argparse
import time
import uuid

import httpx


def submit_job(client: httpx.Client, api_url: str, index: int) -> str:
    idempotency_key = f"load-test-{uuid.uuid4()}"

    response = client.post(
        f"{api_url}/jobs",
        headers={"Idempotency-Key": idempotency_key},
        json={
            "job_type": "sum_numbers",
            "payload": {
                "numbers": [index, index + 1, index + 2]
            },
            "priority": 5,
            "max_retries": 3,
        },
    )

    response.raise_for_status()
    return response.json()["id"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=100)
    args = parser.parse_args()

    started_at = time.time()

    job_ids: list[str] = []

    with httpx.Client(timeout=10.0) as client:
        for index in range(args.jobs):
            job_id = submit_job(client, args.api_url, index)
            job_ids.append(job_id)

            if (index + 1) % 10 == 0:
                print(f"Submitted {index + 1}/{args.jobs} jobs")

    duration = time.time() - started_at

    print()
    print(f"Submitted {len(job_ids)} jobs in {duration:.2f} seconds")
    print(f"Submission rate: {len(job_ids) / duration:.2f} jobs/second")
    print()
    print("First 5 job IDs:")
    for job_id in job_ids[:5]:
        print(job_id)


if __name__ == "__main__":
    main()