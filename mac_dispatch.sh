#!/bin/bash
# Kick the X-scout GitHub pipeline right away (GitHub's own cron can run hours late).
# Usage: dispatch.sh prep|tick|apply   Uses the josephcheung2000-star gh login without switching accounts.
JOB="${1:-tick}"
GH=/opt/homebrew/bin/gh
TOKEN="$($GH auth token -u josephcheung2000-star 2>/dev/null)"
[ -z "$TOKEN" ] && { echo "$(date '+%F %T') no gh token for josephcheung2000-star"; exit 1; }
GH_TOKEN="$TOKEN" $GH workflow run pipeline.yml -R josephcheung2000-star/x-scout -f job="$JOB" \
  && echo "$(date '+%F %T') dispatched $JOB" || echo "$(date '+%F %T') dispatch $JOB FAILED"
