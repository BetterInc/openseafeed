# Production runbook — one Scaleway box

> **How the openseafeed.com instance is deployed.** Bram's production instance
> runs on an existing shared Scaleway Kapsule cluster, wired up by the
> `modules/openseafeed` module in his private `projects-infrastructure` repo (same
> cluster as prod-battle and dark-ships; Wasabi for the ClickHouse cold tier).
> That path is not reproducible outside that repo, so it is documented there,
> not here.
>
> **Everything below is the public route** — the two ways anyone else can stand
> up OpenSeaFeed with only this repository: this runbook (one instance, docker
> compose behind Caddy) and the from-scratch Kubernetes path in
> [`deploy/tofu`](../deploy/tofu) + [`deploy/k8s`](../deploy/k8s) documented in
> [deploy.md](deploy.md). Both are fully supported; pick by scale, not by status.

**One** Scaleway instance in `fr-par`, running the same docker compose stack as
a laptop, behind Caddy for TLS. Object storage (ClickHouse cold tier + backups)
is on Wasabi.

It exists because the Kapsule path in [`deploy/tofu`](../deploy/tofu) +
[`deploy/k8s`](../deploy/k8s) costs €50-70/month plus load balancers before a
single vessel is tracked, and the network does not need three nodes yet. Same
image, same env vars, same services — so moving up later is a migration, not a
rewrite (see [Moving to Kapsule](#moving-to-kapsule)).

| | |
|---|---|
| Compute | 1x Scaleway `PRO2-S` (2 vCPU / 16 GB), Ubuntu 24.04, fr-par-1 |
| Storage | 100 GB block volume mounted at `/var/lib/docker` |
| Edge | Caddy 2, automatic Let's Encrypt certificates |
| Object storage | Wasabi eu-central-2 — cold tier + backups (separate buckets) |
| Cost | ~€30/month compute + storage, ~$7/TB/month Wasabi |
| Files | `deploy/tofu-instance/`, `docker-compose.prod.yml`, `deploy/caddy/`, `.env.production.example`, `scripts/backup.sh` |

## Phase 0 — before you touch any infrastructure

Everything here is a prerequisite of the first deploy, and most of it involves
waiting on someone else (DNS propagation, OAuth app review, bucket creation).

1. **Domain + DNS.** Own the domain and have its zone in Cloudflare. Three A
   records go in by hand once the box has an IP (section 1), all pointing at
   it: `stream.`, `api.`, `ingest.`. Keep `stream.` **unproxied** (grey cloud) —
   long-lived WebSockets do not survive the Cloudflare proxy's idle timeouts,
   and Caddy needs to answer the ACME challenge for that hostname itself.

2. **Wasabi buckets — two of them**, in the Wasabi console (deliberately not
   managed by OpenTofu; we don't hand tofu Wasabi root keys):
   - `openseafeed-ais-cold` — the ClickHouse cold tier. **Live data**, queried
     in place by the archiver's history API.
   - `openseafeed-backup` — nightly backups. A separate bucket on purpose: a
     backup that shares a bucket with the data it protects is not a backup.

   Create one access key pair with read/write on both, and note the region
   endpoint (`https://s3.eu-central-2.wasabisys.com` for Amsterdam).

3. **OAuth apps** with the real callback URLs — they have to match exactly, so
   the domain must be settled first:
   - GitHub: `https://api.<your-domain>/auth/github/callback`
   - Google: `https://api.<your-domain>/auth/google/callback`

4. **Rotate the aisstream.io API key.** The current key has been passed around
   in plaintext, which means it must be treated as compromised: generate a new
   one in the aisstream.io dashboard, revoke the old one, and put only the new
   one in `.env` on the box. Nowhere else.

5. **Generate the platform secrets** (`openssl rand -hex 32` each):
   `OSF_INTERNAL_TOKEN`, `OSF_SESSION_SECRET`, and a strong
   `OSF_CLICKHOUSE_PASSWORD`.

6. **Create real feed keys** for each connector in the control-plane dashboard
   once it is up — production runs `OSF_KEYS_MODE=http`, so the dev-style
   placeholder keys are rejected. One key per feed, so a leak is revoked
   without taking the other feeds down. (Chicken-and-egg: bring the stack up
   without the connector profiles, create the keys, then add the profiles — see
   the end of section 2.)

The full list of what ends up in `.env`, with a one-line explanation each, is
[`.env.production.example`](../.env.production.example). Nothing on this list
belongs in git, in a ticket, or in a chat message.

## 1. Provision the box

```sh
cd deploy/tofu-instance
cp backend.tf.example backend.tf        # optional: remote state
tofu init
tofu apply \
  -var 'ssh_key_name=<your-scaleway-ssh-key-name>' \
  -var 'admin_cidr=<your.ip>/32' \
  -var 'domain=<your-domain>'
```

Details of what gets created — instance, static IP, data volume, security group
— are in [`deploy/tofu-instance/README.md`](../deploy/tofu-instance/README.md).
The important part of the security group: inbound is **drop** by default, with
`80/tcp` + `443/tcp` (Caddy), `10111/tcp` (NMEA over TCP) open to the world and
`22/tcp` open only to `admin_cidr`. There is no `10110/udp` — anonymous UDP
ingest stays a dev/LAN convenience.

Then:

```sh
tofu output public_ip      # -> create the three A records in Cloudflare
tofu output dns_records    # prints exactly which names to create
tofu output -raw ssh       # -> ssh root@<ip>
```

Cloud-init needs 2-3 minutes after the instance reports ready. Confirm before
deploying anything:

```sh
ssh root@<ip> 'cloud-init status --wait && docker compose version && df -h /var/lib/docker'
```

`/var/lib/docker` must show the ~100 GB volume, not the root filesystem — the
image build cache, ClickHouse's hot tier, NATS JetStream, the snapshots and the
control-plane sqlite db all live there. If it shows the root volume, cloud-init
failed to find the block device: check `/var/log/cloud-init-output.log` and
re-run `/usr/local/sbin/osf-bootstrap.sh` (it is idempotent).

## 2. First deploy

There is no registry image yet (the repo is not public), so the box builds it.

```sh
# From your laptop, in the repo root — or `git clone` on the box once public.
# --exclude .env is not optional: without it a re-sync deletes the box's
# secrets. Same command is used for every later deploy.
rsync -az --delete \
  --exclude .git --exclude .env --exclude target --exclude .e2e-venv \
  ./ root@<ip>:/opt/openseafeed/
```

Then on the box:

```sh
cd /opt/openseafeed
cp .env.production.example .env
$EDITOR .env          # fill every CHANGE-ME (Phase 0 has the values)
chmod 600 .env        # it holds every secret the stack has

docker compose up -d --build
```

`.env` sets `COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml`, so plain
`docker compose` commands always pick up the production overlay — no `-f` flags
to forget.

**The first build takes 15-20 minutes** on 2 vCPU: it compiles the whole Rust
workspace. Subsequent builds are far quicker — the Dockerfile's BuildKit cache
mounts keep `target/` and the cargo registry between builds, and they live on
the data volume. Watch it with `docker compose logs -f`.

Bring the connectors up **after** the control plane exists, because
`OSF_KEYS_MODE=http` means their feed keys are validated for real:

1. `docker compose up -d --build` with `COMPOSE_PROFILES` unset or without the
   connector profiles (the platform services alone).
2. Log in at `https://api.<your-domain>/` and create one feed key per connector.
3. Put them in `.env` (`OSF_FEED_KEY_NORWAY`, ...), set
   `COMPOSE_PROFILES=feeds,aisstream,dma-history`, and `docker compose up -d`.

## 3. Verify

Certificates are issued on the first request to each hostname; the first one
can take a few seconds. From anywhere:

```sh
curl -fsS https://api.<your-domain>/healthz       # control
curl -fsS https://stream.<your-domain>/healthz    # fanout
curl -fsS https://ingest.<your-domain>/healthz    # ingest

# The data endpoints all require a real API key from the dashboard (?key=):
curl -fsS "https://api.<your-domain>/v1/snapshot?key=<KEY>" | head -c 200      # snapshotter
curl -fsS "https://api.<your-domain>/v1/history/477553000?key=<KEY>&limit=1"   # archiver
```

The `api.` host is the one to watch: Caddy fans it out to three backends by
path (`/v1/snapshot*` and `/v1/vessels*` to snapshotter, `/v1/history*` to
archiver, everything else to control), so a 404 there is a routing problem
rather than a dead service.

On the box:

```sh
cd /opt/openseafeed
set -a; . ./.env; set +a                # so $OSF_* are available below

docker compose ps                       # every service Up, healthchecks passing
docker compose logs --tail=50 pipeline  # decoded-message counts climbing
docker compose exec -T snapshotter wget -qO- http://localhost:8082/healthz
docker compose exec -T clickhouse clickhouse-client --user osf \
  --password "$OSF_CLICKHOUSE_PASSWORD" \
  --query "SELECT count() FROM osf.positions"
```

Then check the live path end to end — the live map at
`https://stream.<your-domain>/` should fill with vessels within a minute of the
Norway/Finland connectors starting, and a real subscription should deliver
messages:

```sh
pip install websockets     # once, on your laptop
python3 - <<'EOF'
import asyncio, json, websockets
async def main():
    async with websockets.connect("wss://stream.<your-domain>/v1/stream") as ws:
        await ws.send(json.dumps({"APIKey": "<a real key from the dashboard>",
                                  "BoundingBoxes": [[[54.0, 2.0], [62.0, 13.0]]]}))
        for _ in range(3):
            print(json.loads(await ws.recv())["MetaData"])
asyncio.run(main())
EOF
```

**About `scripts/e2e.py`:** it is a *dev* smoke test and cannot be pointed at
production as-is — it feeds NMEA over UDP to `127.0.0.1:10110` and has its
endpoints hardcoded, while production disables anonymous UDP entirely
(`OSF_ALLOW_ANON_UDP=0`) and publishes no UDP port. Run it against `make dev` on
a laptop to validate a build; the curl + WebSocket checks above are its
production equivalent.

## 4. Backups

[`scripts/backup.sh`](../scripts/backup.sh) writes two things to
`OSF_BACKUP_BUCKET` every night:

- **ClickHouse** — native `BACKUP DATABASE osf TO S3(...)`, consistent and
  restorable, named `clickhouse/osf-<UTC timestamp>`.
- **The control-plane sqlite db** — accounts, API keys, stations. The one piece
  of state that cannot be re-derived from the feeds. Taken with
  `sqlite3 .backup` (the db runs in WAL mode, so a plain file copy can miss
  committed transactions), stored as `control/control-<UTC timestamp>.db`.

Install the cron job on the box:

```sh
crontab -e
# Nightly at 03:17 UTC. The script cds to /opt/openseafeed itself, reads .env
# for credentials, and holds a flock so runs cannot overlap (exit 75 = another
# run in progress, which is not a failure).
17 3 * * * /opt/openseafeed/scripts/backup.sh >> /var/log/osf-backup.log 2>&1
```

Run it by hand once and read the output before trusting the schedule:

```sh
/opt/openseafeed/scripts/backup.sh
```

**Retention** is `OSF_BACKUP_KEEP` (default 14), applied by the script itself
at the end of each run: it lists the bucket and deletes everything older than
the newest 14 of each kind. Nothing prunes server-side, so **if the cron job
stops running, old backups accumulate forever** — the bill is the only signal.

The box has no `aws` binary (nor python, nor pip): the script shells out to
`amazon/aws-cli` in a throwaway container, and so should you when poking at the
buckets. Worth keeping in `~/.bashrc` on the box:

```sh
set -a; . /opt/openseafeed/.env; set +a       # load the credentials
osfaws() {
  docker run --rm \
    -e AWS_ACCESS_KEY_ID="$OSF_WASABI_ACCESS_KEY" \
    -e AWS_SECRET_ACCESS_KEY="$OSF_WASABI_SECRET_KEY" \
    -e AWS_DEFAULT_REGION="$OSF_WASABI_REGION" \
    -v "$PWD:/work" -w /work \
    amazon/aws-cli --endpoint-url "$OSF_WASABI_ENDPOINT" "$@"
}

osfaws s3 ls "s3://$OSF_BACKUP_BUCKET/clickhouse/"   # what backups exist
osfaws s3 ls --summarize --human-readable --recursive "s3://$OSF_BACKUP_BUCKET/"
```

Two cost notes specific to Wasabi:

- **90-day minimum billing.** Deleting a backup after 14 days does not stop the
  bill for it; you pay 90 days per object regardless. Nightly *full* ClickHouse
  backups therefore cost roughly six weeks' worth of full copies in steady
  state. If that becomes real money, move ClickHouse to weekly full backups
  (`0 4 * * 0`) and keep the sqlite copy nightly — it is tiny.
- The cold tier already keeps aged parts in Wasabi, so the ClickHouse backup is
  insurance against *deletion and corruption*, not the archive itself.

### Restoring

Both recipes assume the credentials are loaded (`set -a; . /opt/openseafeed/.env;
set +a`) and the `osfaws` helper above is defined.

ClickHouse, from a backup name in the bucket listing:

```sh
docker compose exec -T clickhouse clickhouse-client --user osf --password "$OSF_CLICKHOUSE_PASSWORD" --query "
  RESTORE DATABASE osf FROM S3('$OSF_WASABI_ENDPOINT/$OSF_BACKUP_BUCKET/clickhouse/osf-<STAMP>',
                               '$OSF_WASABI_ACCESS_KEY', '$OSF_WASABI_SECRET_KEY')"
```

Restoring into a database that still exists fails — drop or rename it first, or
restore with `RESTORE DATABASE osf AS osf_restored ...` and compare before
switching.

The control-plane db (stop the service so nothing writes while it is replaced):

```sh
cd /opt/openseafeed
osfaws s3 cp "s3://$OSF_BACKUP_BUCKET/control/control-<STAMP>.db" /work/control.db
docker compose stop control
docker run --rm -v openseafeed_control:/data/control -v "$PWD:/in" alpine:3.20 \
  sh -c 'rm -f /data/control/control.db-wal /data/control/control.db-shm && cp /in/control.db /data/control/control.db'
docker compose start control
```

## 5. Updating and rolling back

Deploying a new version is "get the new source onto the box, rebuild". The
rsync from section 2 is the transport (the box has no `.git`, so there is
nothing to `git pull` unless you cloned it there instead):

```sh
# laptop
rsync -az --delete \
  --exclude .git --exclude .env --exclude target --exclude .e2e-venv \
  ./ root@<ip>:/opt/openseafeed/

# box
cd /opt/openseafeed
docker image tag openseafeed:dev openseafeed:prev   # keep the running build
docker compose up -d --build
docker compose ps
```

Rollback without a rebuild, as long as `openseafeed:prev` still exists:

```sh
docker image tag openseafeed:prev openseafeed:dev
docker compose up -d --force-recreate
```

Rollback to a specific commit (rebuild, ~2-5 minutes with warm caches) — from
the laptop, where the git history lives:

```sh
git log --oneline -20
git checkout <sha>
rsync -az --delete --exclude .git --exclude .env --exclude target \
  --exclude .e2e-venv ./ root@<ip>:/opt/openseafeed/
ssh root@<ip> 'cd /opt/openseafeed && docker compose up -d --build'
git checkout -   # don't leave the laptop on a detached HEAD
```

Data survives all of this: every stateful service uses a named volume on the
data volume. `docker compose down` keeps volumes; `down -v` destroys them, so
never type it on this box.

Once the repo is public, CI publishes `ghcr.io/openseafeed/openseafeed` and this
whole section becomes `docker compose pull && docker compose up -d` with the
image pinned by tag — no compiler on the production box at all. That is the
first thing to change after open-sourcing.

## 6. Day-to-day operations

```sh
docker compose logs -f --tail=100 fanout      # per-service logs (capped at 50MB x3 by the daemon)
docker compose restart pipeline               # single service
docker stats --no-stream                      # what is eating the 16 GB
df -h /var/lib/docker                         # the number that matters
docker system prune -f                        # reclaim dangling build layers
```

Things to watch, roughly in order of likelihood:

- **Disk on `/var/lib/docker`.** ClickHouse's hot window is ~2 GB/day at 1k
  msg/s; the Denmark history importer downloads 500-900 MB files into a
  container tmp volume. If it fills, ClickHouse stops accepting inserts. Shrink
  `OSF_HOT_DAYS` (needs the manual `ALTER TABLE ... MODIFY TTL` from
  [deploy.md](deploy.md#history-storage) — the env var alone does nothing to an
  existing table) or grow the volume in `deploy/tofu-instance`.
- **The first DMA backfill.** `OSF_DMA_MAX_FILES=0` means "fill the whole
  window", which is deliberately heavy. Set it to `1` if it interferes with the
  live path; the rate limit is `OSF_DMA_ROWS_PER_SEC`.
- **Certificate renewals.** Caddy handles them; the only way they fail is port
  80 being blocked or DNS pointing elsewhere. `docker compose logs caddy` says
  so plainly.
- **The `caddy-data` volume.** It holds the certificates and the ACME account
  key. Losing it means re-issuing everything and Let's Encrypt rate limits
  apply.

Ingest's `10111/tcp` is the only service port reachable from off the box: NATS
stays bound to `127.0.0.1` (loopback, so `curl localhost:8222/varz` works over
SSH but nothing else does), and ClickHouse plus every HTTP service are reachable
only inside the compose network or through Caddy. Use `docker compose exec` to
poke at those.

## Moving to Kapsule

Symptoms that the single box is done: sustained CPU saturation on 2 vCPU,
fanout needing to scale past one process, or wanting zero-downtime deploys.
The cluster path is already written and unchanged by any of this:

- [`deploy/tofu`](../deploy/tofu) — Kapsule cluster + pool + Cloudflare DNS.
- [`deploy/k8s`](../deploy/k8s) — kustomize base and prod overlay (nginx
  Ingress, HPA on fanout, PDBs, ClickHouse StatefulSet).
- [`deploy.md`](deploy.md) — the full Kubernetes story, including the
  `clickhouse-storage` ConfigMap that carries the same Wasabi policy this box
  gets from `docker-compose.prod.yml`.

The migration in outline: bring the cluster up alongside the box, move the
secrets from `.env` into `openseafeed-secrets`, point the connectors at the
cluster's ingest endpoint, restore the ClickHouse backup and the control sqlite
db into the cluster, cut DNS over, keep the box for a week, then destroy it.
The `.env` -> Secret mapping is one-to-one: same variable names, same values.
