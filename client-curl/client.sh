#!/usr/bin/env bash
# Connect protocol (JSON) client using only curl, HTTP/1.1 only.
# Flags mirror client-rs (minus --mode):
#   --addr URL          server base URL        (default http://localhost:8080)
#   --cmd CMD           echo | telemetry | subscribe   (default echo)
#   --message TEXT      echo message           (default hello)
#   --batch-size N      telemetry events       (default 5)
#   --batch-id ID       telemetry batch id     (default batch-1)
#   --last-seen-id N    subscribe resume point (default 0)
#   --max N             subscribe: stop after N notifications (default 8, 0 = until server ends)
#   -v, --verbose       log headers (curl -v), request bodies and response envelopes to stderr
set -euo pipefail

ADDR=http://localhost:8080 CMD=echo MESSAGE=hello BATCH_SIZE=5 BATCH_ID=batch-1 LAST_SEEN_ID=0 MAX=8 VERBOSE=0

usage() { sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//' >&2; exit "${1:-2}"; }

while [ $# -gt 0 ]; do
  case $1 in
    -h|--help) usage 0 ;;
    -v|--verbose) VERBOSE=1; shift ;;
    --addr|--cmd|--message|--batch-size|--batch-id|--last-seen-id|--max)
      [ $# -ge 2 ] || { echo "missing value for $1" >&2; usage; }
      case $1 in
        --addr) ADDR=$2 ;;           --cmd) CMD=$2 ;;
        --message) MESSAGE=$2 ;;     --batch-size) BATCH_SIZE=$2 ;;
        --batch-id) BATCH_ID=$2 ;;   --last-seen-id) LAST_SEEN_ID=$2 ;;
        --max) MAX=$2 ;;
      esac
      shift 2 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

SVC=$ADDR/sample.v1.SampleService
CURL=(curl -sS --http1.1 --fail-with-body)
(( VERBOSE )) && CURL+=(-v)
log() { (( VERBOSE )) && echo "$@" >&2 || true; }

case $CMD in
  echo)
    # Unary: plain JSON body, Content-Type: application/json
    body="{\"message\":\"$MESSAGE\"}"
    log "> body (${#body} bytes): $body"
    "${CURL[@]}" -H 'Content-Type: application/json' -d "$body" "$SVC/Echo"; echo
    ;;

  telemetry)
    # Unary with a repeated field: the client-side batch.
    n=$BATCH_SIZE; now=$(date +%s%3N); events=""
    for ((i = 0; i < n; i++)); do
      events+="${events:+,}{\"name\":\"cpu\",\"value\":$i,\"timestampUnixMs\":\"$((now + i))\"}"
    done
    body="{\"batchId\":\"$BATCH_ID\",\"events\":[$events]}"
    log "> body (${#body} bytes): $body"
    "${CURL[@]}" -H 'Content-Type: application/json' -d "$body" "$SVC/ReportTelemetry"; echo
    ;;

  subscribe)
    # Server stream: Content-Type application/connect+json. Request and each
    # response message are wrapped in a 5-byte envelope: 1 flag byte
    # (bit 0x02 = end-of-stream) + 4-byte big-endian length, then the payload.
    body="{\"maxNotifications\":$MAX,\"lastSeenId\":\"$LAST_SEEN_ID\"}"
    len=$(printf %s "$body" | wc -c)
    log "> envelope flags=0x00 len=$len: $body"
    {
      printf '\x00'
      printf "\\x$(printf %02x $((len >> 24 & 255)))\\x$(printf %02x $((len >> 16 & 255)))\\x$(printf %02x $((len >> 8 & 255)))\\x$(printf %02x $((len & 255)))"
      printf %s "$body"
    } | "${CURL[@]}" -N -H 'Content-Type: application/connect+json' \
          --data-binary @- "$SVC/Subscribe" |
    while true; do
      hdr=$(head -c 5 | xxd -p) || true
      [ ${#hdr} -eq 10 ] || break
      flags=$((16#${hdr:0:2})); mlen=$((16#${hdr:2:8}))
      payload=$(head -c "$mlen")
      log "< envelope flags=0x$(printf %02x "$flags") len=$mlen: $payload"
      if (( flags & 2 )); then echo "end-of-stream: $payload"; break; fi
      echo "$payload"
    done
    ;;

  *) echo "unknown --cmd: $CMD" >&2; exit 2 ;;
esac
