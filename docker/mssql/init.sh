#!/bin/sh
# Applies every script in sql/ once SQL Server is accepting connections.
#
# The files are named so that plain alphabetical order is the correct order:
#   01_create_gold         database, gold schema, dimensions, fact
#   01b_create_login       the crypto_app login (must exist before the grants)
#   02_seed_dims           dim_date + dim_time rows
#   03_create_staging      staging.candles_1m
#   04_create_api_staging  API staging tables + dim_date top-up
#   05_create_ops          ops.pipeline_runs
#
# Every script is idempotent, so re-running this service is harmless.
set -eu

# sqlcmd lives in a different place depending on the image and major version,
# and only the 18 build knows -C (trust the server's self-signed certificate).
SQLCMD=""
EXTRA=""
if command -v sqlcmd >/dev/null 2>&1; then
    SQLCMD="$(command -v sqlcmd)"
    EXTRA="-C"
elif [ -x /opt/mssql-tools18/bin/sqlcmd ]; then
    SQLCMD=/opt/mssql-tools18/bin/sqlcmd
    EXTRA="-C"
elif [ -x /opt/mssql-tools/bin/sqlcmd ]; then
    SQLCMD=/opt/mssql-tools/bin/sqlcmd
else
    echo "no sqlcmd in this image - cannot initialise the warehouse" >&2
    exit 1
fi

run() {
    "$SQLCMD" -S "${MSSQL_HOST:-mssql}" -U sa -P "$MSSQL_SA_PASSWORD" $EXTRA -b "$@"
}

echo "waiting for SQL Server..."
i=0
until run -Q "SELECT 1" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "$i" -gt 60 ]; then
        echo "SQL Server did not become ready in time" >&2
        exit 1
    fi
    sleep 5
done

for script in /sql/*.sql; do
    echo "applying $(basename "$script")"
    run -v APP_PASSWORD="$MSSQL_PASSWORD" -i "$script"
done

echo "warehouse ready"
