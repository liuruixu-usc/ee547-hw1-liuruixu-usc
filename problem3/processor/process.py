#!/usr/bin/env python3
import os
import re
import json
import time
from datetime import datetime, timezone

def strip_html(html_content):
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', html_content)
    text = re.sub(r'\s+', ' ', text).strip()
    return text, links, images

def main():
    print(f"[{datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}] Processor starting", flush=True)
    status_file = "/shared/status/fetch_complete.json"
    while not os.path.exists(status_file):
        time.sleep(2)
    os.makedirs("/shared/processed", exist_ok=True)
    os.makedirs("/shared/status", exist_ok=True)
    raw_dir = "/shared/raw"
    processed_count = 0
    for fname in sorted(os.listdir(raw_dir)):
        if not fname.endswith(".html"):
            continue
        fpath = os.path.join(raw_dir, fname)
        try:
            with open(fpath, "r", errors="ignore") as f:
                html = f.read()
        except:
            continue
        text, links, images = strip_html(html)
        words = re.findall(r"[0-9A-Za-z]+", text)
        sentences = [s for s in re.split(r"[.!?]", text) if s.strip()]
        paragraphs = [p for p in re.split(r"\n+", text) if p.strip()]
        stats = {
            "word_count": len(words),
            "sentence_count": len(sentences),
            "paragraph_count": len(paragraphs),
            "avg_word_length": (sum(len(w) for w in words) / len(words)) if words else 0.0
        }
        output = {
            "source_file": fname,
            "text": text,
            "statistics": stats,
            "links": links,
            "images": images,
            "processed_at": datetime.now(timezone.utc).isoformat()
        }
        outpath = os.path.join("/shared/processed", fname.replace(".html", ".json"))
        with open(outpath, "w") as f:
            json.dump(output, f, indent=2)
        processed_count += 1
    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "files_processed": processed_count
    }
    with open("/shared/status/process_complete.json", "w") as f:
        json.dump(status, f, indent=2)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Processor complete", flush=True)

if __name__ == "__main__":
    main()
