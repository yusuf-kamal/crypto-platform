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

# sqlcmd lives in a different place depending on the image and major version.
# In mcr.microsoft.com/mssql/server:2022-latest it is at exactly one path,
# /opt/mssql-tools18/bin/sqlcmd, and it is NOT on PATH -- verified against the
# image, so that path is tried first.
#
# -C (trust the server certificate) is not optional with the 18 tools: driver
# 18 defaults to Encrypt=yes and the container's certificate is self-signed,
# so without it every call fails with
#   SSL Provider: [error:0A000086:...:certificate verify failed:self-signed
#   certificate] / Client unable to establish connection
# which reads like a server that is not up yet, not like a TLS setting. The
# pre-18 tools do not know the flag, hence the split.
SQLCMD=""
EXTRA=""
if [ -x /opt/mssql-tools18/bin/sqlcmd ]; then
    SQLCMD=/opt/mssql-tools18/bin/sqlcmd
    EXTRA="-C"
elif command -v sqlcmd >/dev/null 2>&1; then
    SQLCMD="$(command -v sqlcmd)"
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

# Readiness is not "master answers". On a restart the warehouse already
# exists, and SQL Server accepts connections to master while it is still
# recovering user databases -- the first script then fails with
#
#   Msg 904: Database 5 cannot be autostarted during server shutdown or startup
#
# which looks like a broken script rather than an impatient one. So the probe
# also requires CryptoDWH to be ONLINE, while still passing on a fresh volume
# where it does not exist yet. -b makes sqlcmd exit non-zero on the RAISERROR.
READY_PROBE="SET NOCOUNT ON;
IF DB_ID('CryptoDWH') IS NOT NULL
   AND DATABASEPROPERTYEX('CryptoDWH', 'Status') <> 'ONLINE'
    RAISERROR('warehouse still recovering', 16, 1);
SELECT 1;"

echo "waiting for SQL Server..."
i=0
until run -Q "$READY_PROBE" >/dev/null 2>&1; do
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
