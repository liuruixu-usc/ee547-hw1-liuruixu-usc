#!/usr/bin/env python3
import os
import re
import json
import time
from datetime import datetime, timezone
from collections import Counter
from itertools import islice

def jaccard_similarity(doc1_words, doc2_words):
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def get_ngrams(words, n):
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]

def main():
    print(f"[{datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}] Analyzer starting", flush=True)
    status_file = "/shared/status/process_complete.json"
    while not os.path.exists(status_file):
        time.sleep(2)
    os.makedirs("/shared/analysis", exist_ok=True)
    processed_dir = "/shared/processed"
    docs = []
    all_words = []
    doc_tokens = {}
    sentence_lengths = []
    word_lengths = []
    for fname in sorted(os.listdir(processed_dir)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(processed_dir, fname)) as f:
            data = json.load(f)
        text = data.get("text", "") or ""
        words = re.findall(r"[0-9A-Za-z]+", text.lower())
        sentences = [s for s in re.split(r"[.!?]", text) if s.strip()]
        docs.append(fname)
        doc_tokens[fname] = words
        all_words.extend(words)
        sentence_lengths.extend(len(re.findall(r"[0-9A-Za-z]+", s)) for s in sentences)
        word_lengths.extend(len(w) for w in words)
    counter = Counter(all_words)
    top_100 = counter.most_common(100)
    total_words = len(all_words)
    unique_words = len(set(all_words))
    top_100_words = [
        {"word": w, "count": c, "frequency": (c / total_words) if total_words else 0.0}
        for w, c in top_100
    ]
    similarities = []
    for i, doc1 in enumerate(docs):
        for j in range(i + 1, len(docs)):
            doc2 = docs[j]
            sim = jaccard_similarity(doc_tokens.get(doc1, []), doc_tokens.get(doc2, []))
            similarities.append({"doc1": doc1, "doc2": doc2, "similarity": sim})
    bigram_counter = Counter()
    trigram_counter = Counter()
    for words in doc_tokens.values():
        if not words:
            continue
        bigram_counter.update(get_ngrams(words, 2))
        trigram_counter.update(get_ngrams(words, 3))
    top_bigrams = [{"bigram": bg, "count": c} for bg, c in islice(bigram_counter.most_common(20), 20)]
    top_trigrams = [{"trigram": tg, "count": c} for tg, c in islice(trigram_counter.most_common(20), 20)]
    readability = {
        "avg_sentence_length": (sum(sentence_lengths) / len(sentence_lengths)) if sentence_lengths else 0.0,
        "avg_word_length": (sum(word_lengths) / len(word_lengths)) if word_lengths else 0.0,
        "complexity_score": (unique_words / total_words) if total_words else 0.0
    }
    report = {
        "processing_timestamp": datetime.now(timezone.utc).isoformat(),
        "documents_processed": len(docs),
        "total_words": total_words,
        "unique_words": unique_words,
        "top_100_words": top_100_words,
        "document_similarity": similarities,
        "top_bigrams": top_bigrams,
        "top_trigrams": top_trigrams,
        "readability": readability
    }
    with open("/shared/analysis/final_report.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Analyzer complete", flush=True)
    KEEP_ALIVE_MAX_SEC = 600
    waited = 0
    while waited < KEEP_ALIVE_MAX_SEC:
        time.sleep(2)
        waited += 2

if __name__ == "__main__":
    main()
