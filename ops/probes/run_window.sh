#!/usr/bin/env bash
# Runs the bundled krx probes for one check window and prefixes each output line with "krx ".
# Usage: run_window.sh MORNING|OPEN|CLOSE|EOD|WEEKLY
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "usage: run_window.sh MORNING|OPEN|CLOSE|EOD|WEEKLY" >&2
  exit 2
fi

case "$1" in
  MORNING) plan="p_host:180 p_am:180" ;;
  OPEN|CLOSE) plan="p_live:180" ;;
  EOD) plan="p_eod:1200 p_host:180" ;;
  WEEKLY) plan="p_week:180 p_host:180" ;;
  *) echo "unknown window: $1" >&2; exit 2 ;;
esac

if [ "${PROBE_DRY_RUN:-0}" = "1" ]; then
  for entry in $plan; do
    echo "krx PROBE_PLAN script=${entry%%:*} budget_s=${entry##*:}"
  done
  exit 0
fi

if [ "$1" = "EOD" ]; then
  export D="${D:-$(TZ=Asia/Seoul date +%F)}"
fi

cd "$HOME/krx-alpha" || { echo "krx PROBE_ABORT reason=base_missing"; exit 1; }

result=0
for entry in $plan; do
  name="${entry%%:*}"
  budget="${entry##*:}"
  start="$(date +%s)"
  nice -n 19 timeout "$budget" bash "$SCRIPT_DIR/$name.sh" | sed -u 's/^/krx /'
  statuses=("${PIPESTATUS[@]}")
  rc="${statuses[0]}"
  [ "$rc" -eq 0 ] && rc="${statuses[1]}"
  [ "$result" -eq 0 ] && result="$rc"
  echo "krx PROBE_DONE script=$name rc=$rc elapsed_s=$(( $(date +%s) - start ))"
done
exit "$result"
