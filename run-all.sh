#!/bin/bash

set -euo pipefail

mkdir -p downloaded
mkdir -p generated
for d in downloaded generated; do
    mkdir -p $d/0var
    mkdir -p $d/1var
    mkdir -p $d/2var
    mkdir -p $d/3var
    mkdir -p $d/1var-by-ltla
    mkdir -p $d/2var-by-ltla
    mkdir -p $d/3var-by-ltla
done

#./scripts/get-ltla-geog.sh

#./scripts/get-dims.sh
#python3 scripts/combine-all-dims.py

#python3 scripts/get-data.py
#python3 scripts/get-data-by-ltla.py

python3 scripts/generate-files.py
python3 scripts/generate-files-by-ltla.py

python3 scripts/create-var-code-jsons.py
