#!/usr/bin/env bash
# Download everything the platform needs before the first `docker compose up`.
#
# Why this exists: the first start pulls six third-party images and then builds
# three local ones, and the Airflow image alone fetches about 280 MB of Spark
# jars. On a connection that drops, a partial transfer used to discard a
# ten-minute build (curl exit 18). The Dockerfile retries now, and this script
# does the same one level up: each step is retried, and every step is safe to
# run again because Docker skips what it already has.
#
# Run it once, on the machine that will do the testing, while you do something
# else. Then `docker compose up -d` starts from a warm cache.
#
#   bash scripts/prefetch.sh
#
# Nothing here starts a container or touches .env.
set -uo pipefail

ATTEMPTS=${ATTEMPTS:-4}
PAUSE=${PAUSE:-5}

say() { printf '\n=== %s\n' "$*"; }

# Retries the whole step rather than a single transfer: a `docker pull` that
# failed halfway resumes from the layers it already has, so a repeat is cheap.
retry() {
    local label="$1"; shift
    local attempt=1
    while true; do
        if "$@"; then
            return 0
        fi
        if [ "$attempt" -ge "$ATTEMPTS" ]; then
            printf '!!! %s failed after %d attempts\n' "$label" "$ATTEMPTS" >&2
            return 1
        fi
        printf '... %s failed (attempt %d/%d), retrying in %ds\n' \
            "$label" "$attempt" "$ATTEMPTS" "$PAUSE" >&2
        sleep "$PAUSE"
        attempt=$((attempt + 1))
    done
}

cd "$(dirname "$0")/.."

if ! docker version >/dev/null 2>&1; then
    echo "Docker is not reachable. Start Docker Desktop and run this again." >&2
    exit 1
fi

# A missing .env makes `docker compose` warn on every unset variable and, for
# the image pulls, substitute empty strings. Pulling still works, so this is a
# warning rather than a stop.
if [ ! -f .env ]; then
    echo "note: no .env yet. Pulling works without it; copy .env.example to" >&2
    echo "      .env and fill it in before docker compose up -d." >&2
fi

say "Pulling third-party images (all profiles)"
# --ignore-buildable skips the three images built from this repo; they are the
# next step. Profiles are named explicitly so the streaming and tools images
# come down too, instead of on first use.
retry "docker compose pull" \
    docker compose --profile streaming --profile tools --profile dev \
    pull --ignore-buildable

say "Building the local images (Airflow with the Spark jars, dbt, producer)"
# This is the long one: roughly 280 MB of jars plus the Python layer.
retry "docker compose build" \
    docker compose --profile streaming --profile tools build

say "What is now local"
docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}' \
    | grep -E 'crypto-airflow|crypto-dbt|crypto-streaming|mssql/server|pgsty/silo|postgres|apache/kafka|apache/airflow' \
    || echo "(nothing matched - check the output above for errors)"

say "Verifying the Spark jars landed in the Airflow image"
# Catches the case where the layer was cached from a run that had fewer jars.
expected=9
found=$(docker run --rm --entrypoint sh crypto-airflow:1.0 \
    -c 'ls -1 /opt/spark-jars/*.jar 2>/dev/null | wc -l' 2>/dev/null | tr -d '[:space:]')
if [ "${found:-0}" = "$expected" ]; then
    echo "ok: $found jars in /opt/spark-jars"
else
    echo "WARNING: expected $expected jars, found ${found:-0}." >&2
    echo "Rebuild with: docker compose build --no-cache airflow-apiserver" >&2
    exit 1
fi

say "Done. docker compose up -d will now start from a warm cache."
