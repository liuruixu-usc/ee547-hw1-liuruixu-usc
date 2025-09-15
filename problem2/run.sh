#!/bin/bash
if [ $# -ne 3 ]; then
    echo "Usage: $0 <query> <max_results> <output_directory>"
    exit 1
fi

QUERY="$1"
MAX_RESULTS="$2"
OUTPUT_DIR="$3"

# 验证 max_results
if ! [[ "$MAX_RESULTS" =~ ^[0-9]+$ ]]; then
    echo "Error: max_results must be an integer"
    exit 1
fi

if [ "$MAX_RESULTS" -lt 1 ] || [ "$MAX_RESULTS" -gt 100 ]; then
    echo "Error: max_results must be between 1 and 100"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

docker run --rm \
    --name arxiv-processor \
    -v "$(realpath $OUTPUT_DIR)":/data/output \
    arxiv-processor:latest \
    "$QUERY" "$MAX_RESULTS" "/data/output"
