#!/bin/bash
set -euo pipefail

# this script assumes it is run from the repo root

if ! command -v uv >/dev/null 2>&1; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

FILE=./databeta-2021-03-30.tgz
if [ -f "$FILE" ]; then
    echo "$FILE already downloaded."
else
    echo "Downloading EPA CEMS data (several GB)..."
    wget https://sandbox.zenodo.org/record/764417/files/databeta-2021-03-30.tgz
fi

# the tar archive has gigabytes of docker image and EIA/FERC data not needed here
# Extract only CEMS parquet files
mkdir -p ./data_in
if [ -d ./data_in/epacems ]; then
    echo "EPA CEMS data already extracted."
else
    echo "Extracting EPA CEMS data..."
    tar -xvzf ./databeta-2021-03-30.tgz databeta-2021-03-30/pudl_data/parquet/epacems
    mv ./databeta-2021-03-30/pudl_data/parquet/epacems/ ./data_in
    rm -rf ./databeta-2021-03-30
fi

echo "EPA_CEMS_DATA_PATH=$(pwd)/data_in/epacems" > .env

echo "Creating uv environment and installing package..."
uv sync --extra dev
uv pip install -e .
