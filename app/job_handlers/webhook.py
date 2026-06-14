from typing import Any

import httpx


def run(payload: dict[str, Any]) -> dict[str, Any]:
    url = str(payload["url"])
    method = payload.get("method", "POST")
    body = payload.get("body")

    with httpx.Client(timeout=10.0) as client:
        if method == "GET":
            response = client.get(url)
        elif method == "POST":
            response = client.post(url, json=body)
        else:
            raise ValueError(f"Unsupported webhook method: {method}")

    return {
        "url": url,
        "method": method,
        "status_code": response.status_code,
        "success": 200 <= response.status_code < 300,
        "response_preview": response.text[:500],
    }