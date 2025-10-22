#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
  exec python app.py "$@"
fi

TIEMPO_VALUE="${TIEMPO:-300}"
TEST_MODE="${TEST_MODE:-0}"

exec python app.py --tiempo "$TIEMPO_VALUE" --test "$TEST_MODE"
