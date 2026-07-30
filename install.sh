#!/bin/bash
set -euo pipefail

# this script assumes it is run from the repo root

if ! command -v uv >/dev/null 2>&1; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

mkdir -p ./data_in

SOURCE_URL="https://s3.us-west-2.amazonaws.com/pudl.catalyst.coop/nightly/core_epacems__hourly_emissions.parquet"
START_DATE="${START_DATE:-2015-01-01}"
END_DATE="${END_DATE:-}"

if [ -n "$END_DATE" ]; then
    RANGE_SUFFIX="${START_DATE}_to_${END_DATE}"
else
    RANGE_SUFFIX="${START_DATE}_to_present"
fi
FILTERED_FILE="./data_in/core_epacems__hourly_emissions_${RANGE_SUFFIX}.parquet"

if [ -f "$FILTERED_FILE" ]; then
    echo "$FILTERED_FILE already exists."
else
    if [ -n "$END_DATE" ]; then
        echo "Materializing EPA CEMS parquet for ${START_DATE} <= operating_datetime_utc < ${END_DATE}..."
    else
        echo "Materializing EPA CEMS parquet for operating_datetime_utc >= ${START_DATE}..."
    fi

    SOURCE_URL="$SOURCE_URL" START_DATE="$START_DATE" END_DATE="$END_DATE" FILTERED_FILE="$FILTERED_FILE" \
    uvx --with duckdb python - <<'PY'
import os

import duckdb

source = os.environ["SOURCE_URL"]
start_date = os.environ["START_DATE"]
end_date = os.environ["END_DATE"]
target = os.environ["FILTERED_FILE"]

con = duckdb.connect()
if end_date:
    sql = """
    COPY (
        SELECT *
        FROM read_parquet(?)
        WHERE operating_datetime_utc >= CAST(? AS TIMESTAMP)
          AND operating_datetime_utc < CAST(? AS TIMESTAMP)
    ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    params = [source, start_date, end_date, target]
else:
    sql = """
    COPY (
        SELECT *
        FROM read_parquet(?)
        WHERE operating_datetime_utc >= CAST(? AS TIMESTAMP)
    ) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    params = [source, start_date, target]

con.execute(sql, params)
con.close()
PY
fi

echo "EPA_CEMS_DATA_PATH=$(pwd)/${FILTERED_FILE#./}" > .env

echo "Creating uv environment and installing package..."
uv sync --extra dev
uv pip install -e .
