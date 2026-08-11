#!/usr/bin/env bash
# Nightly backup for the single-box production deploy (docs/production.md).
#
# Two things are backed up, both to a Wasabi bucket that is SEPARATE from the
# ClickHouse cold tier (a backup in the same bucket as the live data is not a
# backup):
#
#   1. ClickHouse: native `BACKUP DATABASE osf TO S3(...)` — consistent, and
#      restorable with the matching RESTORE statement.
#   2. The control-plane sqlite db (accounts, API keys, stations) — the one
#      piece of state that cannot be re-derived from the feeds.
#
# Usage:
#   scripts/backup.sh                # reads /opt/openseafeed/.env
#   OSF_REPO_DIR=/srv/osf scripts/backup.sh
#
# Cron (see docs/production.md for the exact line):
#   17 3 * * * /opt/openseafeed/scripts/backup.sh >> /var/log/osf-backup.log 2>&1
#
# Required in .env: OSF_WASABI_ENDPOINT, OSF_WASABI_ACCESS_KEY,
# OSF_WASABI_SECRET_KEY, OSF_BACKUP_BUCKET, OSF_CLICKHOUSE_PASSWORD.
set -euo pipefail

REPO_DIR="${OSF_REPO_DIR:-/opt/openseafeed}"
KEEP="${OSF_BACKUP_KEEP:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
AWS_IMAGE="${OSF_AWS_CLI_IMAGE:-amazon/aws-cli}"

log() { printf '%s backup: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() {
  printf '%s backup: ERROR %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
  exit 1
}

# One run at a time: a backup that overlaps the previous one just fights it for
# disk and bandwidth. Re-exec under flock unless we already hold it. Exit code
# 75 means "another run holds the lock", so cron mail can tell that apart from a
# real failure.
if [ -z "${OSF_BACKUP_LOCKED:-}" ]; then
  export OSF_BACKUP_LOCKED=1
  exec flock -n -E 75 /tmp/osf-backup.lock "$0" "$@"
fi

cd "$REPO_DIR" || die "repo not found at $REPO_DIR (set OSF_REPO_DIR)"

# .env holds the credentials; cron has almost no environment of its own.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

for var in OSF_WASABI_ENDPOINT OSF_WASABI_ACCESS_KEY OSF_WASABI_SECRET_KEY \
  OSF_BACKUP_BUCKET OSF_CLICKHOUSE_PASSWORD; do
  [ -n "${!var:-}" ] || die "$var is not set (see .env.production.example)"
done

BUCKET="$OSF_BACKUP_BUCKET"
ENDPOINT="$OSF_WASABI_ENDPOINT"
CH_DB="${OSF_CLICKHOUSE_DB:-osf}"
CH_USER="${OSF_CLICKHOUSE_USER:-osf}"

export AWS_ACCESS_KEY_ID="$OSF_WASABI_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$OSF_WASABI_SECRET_KEY"
export AWS_DEFAULT_REGION="${OSF_WASABI_REGION:-eu-central-2}"

# aws-cli in a throwaway container so the box needs no python/pip. Credentials
# go in as env vars, never on the command line. Extra `docker run` arguments
# (volume mounts) can be passed in AWS_RUN_ARGS.
aws_cli() {
  # shellcheck disable=SC2086
  docker run --rm \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION \
    ${AWS_RUN_ARGS:-} "$AWS_IMAGE" --endpoint-url "$ENDPOINT" "$@"
}

# ClickHouse is not published on the host in production (docker-compose.prod.yml
# resets its ports), so talk to it over HTTP when a URL is reachable — a dev box
# with :8123 published, or a remote server — and fall back to the client inside
# the container otherwise.
CH_URL="${OSF_CLICKHOUSE_URL:-http://127.0.0.1:8123}"
if curl -fsS --max-time 5 "$CH_URL/ping" >/dev/null 2>&1; then
  CH_MODE=http
else
  CH_MODE=compose
fi

ch_query() {
  local sql="$1"
  case "$CH_MODE" in
    http)
      # 6h ceiling: a first full backup of a large hot window is not quick.
      printf '%s' "$sql" | curl -sS -f --max-time 21600 \
        -H "X-ClickHouse-User: $CH_USER" \
        -H "X-ClickHouse-Key: $OSF_CLICKHOUSE_PASSWORD" \
        --data-binary @- "$CH_URL/"
      ;;
    compose)
      docker compose exec -T clickhouse clickhouse-client \
        --user "$CH_USER" --password "$OSF_CLICKHOUSE_PASSWORD" \
        --query "$sql"
      ;;
  esac
}

log "start (stamp=$STAMP, mode=$CH_MODE, bucket=$BUCKET, keep=$KEEP)"

# --- 1. ClickHouse -----------------------------------------------------------
# BACKUP is synchronous here on purpose: we want a non-zero exit when it fails,
# not a job id to chase. The S3 credentials appear in the statement but are
# masked in query_log/logs by ClickHouse's secret-hiding rules.
CH_TARGET="$ENDPOINT/$BUCKET/clickhouse/${CH_DB}-${STAMP}"
log "clickhouse: BACKUP DATABASE $CH_DB -> s3://$BUCKET/clickhouse/${CH_DB}-${STAMP}"
ch_query "BACKUP DATABASE \`$CH_DB\` TO S3('$CH_TARGET', '$OSF_WASABI_ACCESS_KEY', '$OSF_WASABI_SECRET_KEY')" \
  || die "clickhouse backup failed"
log "clickhouse: done"

# --- 2. Control-plane sqlite -------------------------------------------------
# The db runs in WAL mode (services/control/src/db.rs), so copying control.db on
# its own can miss committed transactions still sitting in the -wal file.
# `sqlite3 .backup` takes a consistent snapshot of a live database; sqlite comes
# from a throwaway alpine container so the host needs nothing installed. The
# volume is mounted read-write because sqlite has to attach to the WAL, not
# because the backup changes anything.
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Prefer the volume the running container actually uses; fall back to compose's
# default name (project + "_control"), which is the directory name unless
# COMPOSE_PROJECT_NAME says otherwise.
PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$PWD" | tr '[:upper:]' '[:lower:]')}"
VOLUME="${OSF_CONTROL_VOLUME:-}"
if [ -z "$VOLUME" ]; then
  cid="$(docker compose ps -q control 2>/dev/null || true)"
  if [ -n "$cid" ]; then
    VOLUME="$(docker inspect --format \
      '{{range .Mounts}}{{if eq .Destination "/data/control"}}{{.Name}}{{end}}{{end}}' \
      "$cid" 2>/dev/null || true)"
  fi
fi
VOLUME="${VOLUME:-${PROJECT}_control}"

ARTIFACT="control.db"
ARTIFACT_KEY="control-${STAMP}.db"
log "control: snapshotting $VOLUME:/data/control/control.db"
docker run --rm \
  -v "$VOLUME:/data/control" \
  -v "$WORK:/out" \
  alpine:3.20 sh -c '
    set -e
    apk add --no-cache -q sqlite || exit 90
    sqlite3 /data/control/control.db ".backup /out/control.db"
  ' || {
  rc=$?
  if [ "$rc" -eq 90 ]; then
    # No package mirror (or no network): fall back to a crash-consistent copy of
    # the whole file set — db + -wal + -shm — which sqlite recovers from on open.
    # Keep them together in one tar so a restore cannot pick up a lone db file.
    log "control: sqlite unavailable, falling back to a raw db+WAL tarball"
    docker run --rm -v "$VOLUME:/data/control:ro" -v "$WORK:/out" alpine:3.20 \
      tar czf /out/control-raw.tar.gz -C /data/control . || die "control copy failed"
    ARTIFACT="control-raw.tar.gz"
    ARTIFACT_KEY="control-${STAMP}.raw.tar.gz"
  else
    die "control snapshot failed (rc=$rc)"
  fi
}

log "control: uploading $ARTIFACT_KEY"
AWS_RUN_ARGS="-v $WORK:/backup:ro" aws_cli --only-show-errors \
  s3 cp "/backup/$ARTIFACT" "s3://$BUCKET/control/$ARTIFACT_KEY" \
  || die "control upload failed"
log "control: done"

# --- 3. Retention ------------------------------------------------------------
# Keep the newest $KEEP of each kind. Names are UTC timestamps, so lexical sort
# is chronological. Nothing prunes server-side: this is the only place it
# happens, so a machine that stops running the cron job keeps everything.
#
# NOTE Wasabi bills every object for a minimum of 90 days, so deleting a backup
# at 14 days does not stop the bill for it — see docs/production.md for the
# cost/cadence trade-off.
prune() {
  local prefix="$1" field="$2" recursive="$3"
  local -a entries=()
  while IFS= read -r name; do
    if [ -n "$name" ]; then entries+=("$name"); fi
  done < <(aws_cli s3 ls "s3://$BUCKET/$prefix" 2>/dev/null |
    awk -v f="$field" '{ print $f }' | sed 's#/$##' | sort)

  local total=${#entries[@]}
  if [ "$total" -le "$KEEP" ]; then
    log "prune $prefix: $total kept, nothing to remove"
    return 0
  fi
  local stale=$((total - KEEP))
  log "prune $prefix: $total present, removing the oldest $stale"
  local i
  for ((i = 0; i < stale; i++)); do
    if [ "$recursive" = "yes" ]; then
      aws_cli --only-show-errors s3 rm --recursive "s3://$BUCKET/$prefix${entries[i]}" || true
    else
      aws_cli --only-show-errors s3 rm "s3://$BUCKET/$prefix${entries[i]}" || true
    fi
  done
}

# ClickHouse backups are directories (field 2 = PRE name); control copies are
# plain objects (field 4 = key name).
prune "clickhouse/" 2 yes
prune "control/" 4 no

log "finished ok"
