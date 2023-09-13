#!/bin/bash

### Download a list of LTLAs

set -euo pipefail

mkdir -p downloaded

curl -o downloaded/ltla-geog.json 'https://api.beta.ons.gov.uk/v1/population-types/UR/area-types/ltla/areas?limit=1000'
