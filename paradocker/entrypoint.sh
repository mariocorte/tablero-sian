#!/bin/sh
set -eu

COMMAND="${1:-server}"
shift || true

case "$COMMAND" in
  server)
    PORT="${PORT:-8000}"
    exec uvicorn app:app --host 0.0.0.0 --port "$PORT" "$@"
    ;;
  job)
    exec /app/paradocker/run_job.sh "$@"
    ;;
  *)
    exec "$COMMAND" "$@"
    ;;
esac
