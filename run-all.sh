#!/bin/bash

set -euo pipefail

./scripts/get-ltla-geog.sh

#./scripts/get-dims.sh
python3 scripts/combine-all-dims.py

#python3 scripts/get-data-by-ltla.py
#python3 scripts/get-data.py
