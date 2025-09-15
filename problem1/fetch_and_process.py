#!/usr/bin/env python3
import sys
import os
import json
import time
import re
from datetime import datetime, timezone
from urllib import request
from urllib.error import HTTPError, URLError

WORD_PATTERN = re.compile(r"[0-9A-Za-z]+")

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat() + "Z"

def is_text_response(headers: dict) -> bool:
    ctype = headers.get("Content-Type") or headers.get("content-type") or ""
    return "text" in ctype.lower()

def get_charset(headers: dict) -> str:
    ctype = headers.get("Content-Type") or headers.get("content-type") or ""
    match = re.search(r"charset=([^\s;]+)", ctype, flags=re.IGNORECASE)
    return match.group(1) if match else "utf-8"

def count_words(text: str) -> int:
    return len(WORD_PATTERN.findall(text))

def fetch_one(url: str, timeout_sec: int = 10):
    t0 = time.perf_counter()
    timestamp = utc_now_iso()

    status_code = None
    content_length = 0
    word_count = None
    err_msg = None

    try:
        req = request.Request(url)
        with request.urlopen(req, timeout=timeout_sec) as resp:
            status_code = resp.getcode()
            body = resp.read()
            content_length = len(body)

            headers = {k: v for k, v in resp.getheaders()}
            if is_text_response(headers):
                charset = get_charset(headers)
                try:
                    text = body.decode(charset, errors="replace")
                except LookupError:
                    text = body.decode("utf-8", errors="replace")
                word_count = count_words(text)
            else:
                word_count = None

    except HTTPError as e:
        status_code = e.code
        err_msg = f"HTTP Error {e.code}: {e.reason}"
        content_length = 0
        word_count = None
    except URLError as e:
        err_msg = f"URL Error: {e.reason}"
    except Exception as e:
        err_msg = str(e)

    t1 = time.perf_counter()
    response_time_ms = (t1 - t0) * 1000.0

    return {
        "url": url,
        "status_code": status_code if status_code is not None else None,
        "response_time_ms": response_time_ms,
        "content_length": content_length,
        "word_count": word_count,
        "timestamp": timestamp,
        "error": err_msg if err_msg is not None else None,
    }

def aggregate_summary(results, start_iso, end_iso):
    total = len(results)
    failed = sum(1 for r in results if r["error"] is not None)
    success = total - failed

    avg_rt = sum(r["response_time_ms"] for r in results) / total if total else 0.0
    total_bytes = sum(r["content_length"] for r in results if r["content_length"] is not None)

    dist = {}
    for r in results:
        code = r.get("status_code")
        if code is not None:
            key = str(code)
            dist[key] = dist.get(key, 0) + 1

    return {
        "total_urls": total,
        "successful_requests": success,
        "failed_requests": failed,
        "average_response_time_ms": avg_rt,
        "total_bytes_downloaded": total_bytes,
        "status_code_distribution": dist,
        "processing_start": start_iso,
        "processing_end": end_iso,
    }

def main():
    if len(sys.argv) != 3:
        print("Usage: python fetch_and_process.py <input_file> <output_dir>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.isfile(input_file):
        print(f"Error: Input file does not exist: {input_file}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    processing_start = utc_now_iso()
    results = []
    errors = []

    for url in urls:
        res = fetch_one(url, timeout_sec=10)
        results.append(res)
        if res["error"] is not None:
            errors.append(f"[{utc_now_iso()}] [{url}]: {res['error']}")

    processing_end = utc_now_iso()

    responses_path = os.path.join(output_dir, "responses.json")
    with open(responses_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    summary = aggregate_summary(results, processing_start, processing_end)
    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    errors_path = os.path.join(output_dir, "errors.log")
    with open(errors_path, "w", encoding="utf-8") as f:
        for line in errors:
            f.write(line + "\n")

    return 0

if __name__ == "__main__":
    sys.exit(main())
