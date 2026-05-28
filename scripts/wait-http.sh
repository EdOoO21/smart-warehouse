#!/usr/bin/env sh
set -eu

url="$1"
attempts="${2:-60}"

i=1
while [ "$i" -le "$attempts" ]; do
  if curl -fsS "$url" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
  i=$((i + 1))
done

echo "timeout waiting for $url" >&2
exit 1
