import sys
import os
import json
import time
import re
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone

STOPWORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
    'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
    'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
    'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
    'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
    'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'
}

ATOM_NS = {'atom': 'http://www.w3.org/2005/Atom'}
ARXIV_API = 'http://export.arxiv.org/api/query'

WORD_PATTERN = re.compile(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*")
SENT_SPLIT_PATTERN = re.compile(r"[.!?]+")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

class Logger:
    def __init__(self, log_path: str):
        self.log_path = log_path
        ensure_dir(os.path.dirname(log_path))

    def write(self, msg: str) -> None:
        line = f"[{utc_now_iso()}] {msg}\n"
        with open(self.log_path, 'a', encoding='utf-8') as f:
            f.write(line)
        print(line, end='')

def fetch_arxiv(query: str, max_results: int, logger: Logger, retries: int = 3, backoff_sec: int = 3) -> bytes:
    params = f"search_query={query}&start=0&max_results={max_results}"
    url = f"{ARXIV_API}?{params}"

    attempt = 0
    while attempt <= retries:
        attempt += 1
        req = urllib.request.Request(url, method='GET')
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                content = resp.read()
                logger.write(f"Fetched {max_results} results from ArXiv API")
                return content
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt <= retries:
                logger.write(f"HTTP 429 received. Throttling {backoff_sec}s before retry {attempt}/{retries}...")
                time.sleep(backoff_sec)
                continue
            logger.write(f"HTTP error: {e.code} {e.reason}")
            break
        except urllib.error.URLError as e:
            logger.write(f"Network error: {e.reason}")
            if attempt <= retries:
                logger.write(f"Retrying in {backoff_sec}s (attempt {attempt}/{retries})...")
                time.sleep(backoff_sec)
                continue
            break
        except Exception as e:
            logger.write(f"Unexpected error: {str(e)}")
            break
    return b''

def parse_entries(xml_bytes: bytes, logger: Logger):
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        logger.write(f"Invalid XML: {str(e)}")
        return []

    entries = []
    for entry in root.findall('atom:entry', ATOM_NS):
        try:
            raw_id = entry.findtext('atom:id', default='', namespaces=ATOM_NS)
            if not raw_id:
                logger.write("Warning: Missing <id> — skipping entry")
                continue
            arxiv_id = raw_id.rsplit('/', 1)[-1]

            title = (entry.findtext('atom:title', default='', namespaces=ATOM_NS) or '').strip()
            summary = (entry.findtext('atom:summary', default='', namespaces=ATOM_NS) or '').strip()
            published = entry.findtext('atom:published', default='', namespaces=ATOM_NS) or ''
            updated = entry.findtext('atom:updated', default='', namespaces=ATOM_NS) or ''

            authors = []
            for a in entry.findall('atom:author', ATOM_NS):
                name = a.findtext('atom:name', default='', namespaces=ATOM_NS)
                if name:
                    authors.append(name.strip())

            categories = []
            for c in entry.findall('atom:category', ATOM_NS):
                term = c.get('term')
                if term:
                    categories.append(term)

            if not (title and summary and authors):
                logger.write(f"Warning: Missing required fields for {arxiv_id} — skipping entry")
                continue

            entries.append({
                'arxiv_id': arxiv_id,
                'title': title,
                'authors': authors,
                'abstract': summary,
                'categories': categories,
                'published': published,
                'updated': updated,
            })
        except Exception as e:
            logger.write(f"Error parsing entry: {str(e)} — skipping entry")
            continue
    return entries

def tokenize_words(text: str):
    return WORD_PATTERN.findall(text)

def sentence_split(text: str):
    parts = [s.strip() for s in SENT_SPLIT_PATTERN.split(text)]
    return [s for s in parts if s]

def abstract_stats(abstract: str):
    words = tokenize_words(abstract)
    word_count = len(words)
    sentences = sentence_split(abstract)
    sentence_count = len(sentences)

    avg_wps = sum(len(tokenize_words(s)) for s in sentences) / sentence_count if sentence_count > 0 else 0.0
    avg_wlen = sum(len(w) for w in words) / word_count if word_count > 0 else 0.0

    words_lower = [w.lower() for w in words]
    freq = Counter(words_lower)

    uppercase_terms = sorted({w for w in words if any(c.isupper() for c in w)})
    numeric_terms = sorted({w for w in words if any(c.isdigit() for c in w)})
    hyphenated_terms = sorted({w for w in words if '-' in w})

    top20 = [(w, c) for w, c in freq.most_common() if w not in STOPWORDS][:20]

    return {
        'total_words': word_count,
        'unique_words': len(set(words_lower)),
        'total_sentences': sentence_count,
        'avg_words_per_sentence': round(avg_wps, 3),
        'avg_word_length': round(avg_wlen, 3),
        '_freq': freq,
        '_set_lower': set(words_lower),
        '_uppercase_terms': uppercase_terms,
        '_numeric_terms': numeric_terms,
        '_hyphenated_terms': hyphenated_terms,
        '_top20': top20,
    }

def write_json(path: str, obj) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def process(query: str, max_results: int, output_dir: str) -> int:
    ensure_dir(output_dir)
    logger = Logger(os.path.join(output_dir, 'processing.log'))

    start_ts = time.time()
    logger.write(f"Starting ArXiv query: {query}")

    xml_bytes = fetch_arxiv(query, max_results, logger)
    if not xml_bytes:
        logger.write("Failed to fetch ArXiv API after retries. Exiting with code 1.")
        return 1

    papers = parse_entries(xml_bytes, logger)
    logger.write(f"Parsed {len(papers)} valid entries from XML")

    papers_out = []
    corpus_freq = Counter()
    corpus_doc_count = defaultdict(int)
    corpus_words_total = 0
    corpus_unique_global = set()
    uppercase_global = set()
    numeric_global = set()
    hyphen_global = set()
    category_dist = Counter()

    for p in papers:
        logger.write(f"Processing paper: {p['arxiv_id']}")
        stats = abstract_stats(p['abstract'])

        corpus_freq.update(stats['_freq'])
        for w in stats['_set_lower']:
            corpus_doc_count[w] += 1
        corpus_words_total += stats['total_words']
        corpus_unique_global.update(stats['_set_lower'])
        uppercase_global.update(stats['_uppercase_terms'])
        numeric_global.update(stats['_numeric_terms'])
        hyphen_global.update(stats['_hyphenated_terms'])
        for c in p.get('categories', []):
            category_dist[c] += 1

        paper_obj = {
            'arxiv_id': p['arxiv_id'],
            'title': p['title'],
            'authors': p['authors'],
            'abstract': p['abstract'],
            'categories': p['categories'],
            'published': p['published'],
            'updated': p['updated'],
            'abstract_stats': {
                'total_words': stats['total_words'],
                'unique_words': stats['unique_words'],
                'total_sentences': stats['total_sentences'],
                'avg_words_per_sentence': stats['avg_words_per_sentence'],
                'avg_word_length': stats['avg_word_length'],
            }
        }
        papers_out.append(paper_obj)

    write_json(os.path.join(output_dir, 'papers.json'), papers_out)

    n_docs = len(papers_out)
    avg_abstract_len = corpus_words_total / n_docs if n_docs > 0 else 0.0
    longest_abs = max((p['abstract_stats']['total_words'] for p in papers_out), default=0)
    shortest_abs = min((p['abstract_stats']['total_words'] for p in papers_out), default=0)

    top50_items = [(w, c) for w, c in corpus_freq.most_common() if w not in STOPWORDS][:50]
    top50 = [ {'word': w, 'frequency': c, 'documents': corpus_doc_count.get(w, 0)} for w, c in top50_items ]

    corpus_out = {
        'query': query,
        'papers_processed': n_docs,
        'processing_timestamp': utc_now_iso(),
        'corpus_stats': {
            'total_abstracts': n_docs,
            'total_words': corpus_words_total,
            'unique_words_global': len(corpus_unique_global),
            'avg_abstract_length': round(avg_abstract_len, 3),
            'longest_abstract_words': longest_abs,
            'shortest_abstract_words': shortest_abs,
        },
        'top_50_words': top50,
        'technical_terms': {
            'uppercase_terms': sorted(uppercase_global),
            'numeric_terms': sorted(numeric_global),
            'hyphenated_terms': sorted(hyphen_global),
        },
        'category_distribution': dict(sorted(category_dist.items(), key=lambda x: (-x[1], x[0])))
    }

    write_json(os.path.join(output_dir, 'corpus_analysis.json'), corpus_out)

    elapsed = time.time() - start_ts
    logger.write(f"Completed processing: {n_docs} papers in {elapsed:.2f} seconds")
    return 0

def main():
    if len(sys.argv) != 4:
        print("Usage: arxiv_processor.py <query> <max_results> <output_dir>", file=sys.stderr)
        print("Example: arxiv_processor.py 'cat:cs.LG' 10 ./output", file=sys.stderr)
        sys.exit(2)

    query = sys.argv[1]
    max_results_str = sys.argv[2]
    output_dir = sys.argv[3]

    if not max_results_str.isdigit():
        print("Error: max_results must be a positive integer", file=sys.stderr)
        sys.exit(2)
    max_results = int(max_results_str)
    if not (1 <= max_results <= 100):
        print("Error: max_results must be between 1 and 100", file=sys.stderr)
        sys.exit(2)

    code = process(query, max_results, output_dir)
    sys.exit(code)

if __name__ == '__main__':
    main()