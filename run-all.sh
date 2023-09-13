#!/bin/bash

set -euo pipefail

mkdir -p downloaded
mkdir -p generated
for d in downloaded generated; do
    mkdir -p $d/0var
    mkdir -p $d/1var
    mkdir -p $d/2var
    mkdir -p $d/3var
    mkdir -p $d/0var-by-ltla
    mkdir -p $d/1var-by-ltla
    mkdir -p $d/2var-by-ltla
    mkdir -p $d/3var-by-ltla
done

./bash-scripts/get-ltla-geog.sh

./bash-scripts/get-dims.sh
python3 python-scripts/combine-all-dims.py

python3 python-scripts/get-data.py --skip-existing
python3 python-scripts/get-data-by-ltla.py --skip-existing

python3 python-scripts/generate-files.py
python3 python-scripts/generate-files-by-ltla.py
python3 python-scripts/combine-jsons-for-bars-and-maps.py

python3 python-scripts/create-var-code-jsons.py
