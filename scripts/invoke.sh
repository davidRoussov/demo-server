#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 /path/to/your/data.csv"
  exit 1
fi

FILE_PATH="$1"
URL="http://localhost:8000/typeconverter/uploads/"

curl -X POST "$URL" \
  -F "file=@${FILE_PATH}" \
  -H "Content-Type: multipart/form-data"

