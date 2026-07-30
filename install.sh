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
RAW_FILE=./data_in/core_epacems__hourly_emissions.parquet
FILTERED_FILE=./data_in/core_epacems__hourly_emissions_2015_present.parquet

if [ -f "$RAW_FILE" ]; then
    echo "$RAW_FILE already downloaded."
else
    echo "Downloading EPA CEMS parquet from PUDL nightly..."
    curl -L "$SOURCE_URL" -o "$RAW_FILE"
fi

if [ -f "$FILTERED_FILE" ]; then
    echo "$FILTERED_FILE already exists."
else
    echo "Filtering EPA CEMS parquet to operating_datetime_utc >= 2015-01-01..."
    uvx --with pyarrow python - <<'PY'
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

source = "./data_in/core_epacems__hourly_emissions.parquet"
target = "./data_in/core_epacems__hourly_emissions_2015_present.parquet"

dataset = ds.dataset(source, format="parquet")
scanner = dataset.scanner(batch_size=250_000)

writer = None
for batch in scanner.to_batches():
    if "operating_datetime_utc" not in batch.schema.names:
        raise KeyError("Column operating_datetime_utc was not found in source parquet.")

    years = pc.year(batch.column("operating_datetime_utc"))
    mask = pc.greater_equal(years, pa.scalar(2015, type=pa.int64()))
    filtered = batch.filter(mask)
    if filtered.num_rows == 0:
        continue

    table = pa.Table.from_batches([filtered])
    if writer is None:
        writer = pq.ParquetWriter(target, table.schema)
    writer.write_table(table)

if writer is None:
    raise RuntimeError("No rows found on or after 2015-01-01 in source parquet.")

writer.close()
PY
fi

echo "EPA_CEMS_DATA_PATH=$(pwd)/data_in/core_epacems__hourly_emissions_2015_present.parquet" > .env

echo "Creating uv environment and installing package..."
uv sync --extra dev
uv pip install -e .
