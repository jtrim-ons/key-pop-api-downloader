#!/bin/bash

set -euo pipefail

mkdir -p downloaded

curl -o downloaded/poptypes.json 'https://api.beta.ons.gov.uk/v1/population-types?limit=100'

curl -o downloaded/dimensions.json 'https://api.beta.ons.gov.uk/v1/population-types/UR/dimensions?limit=100'

jq '.items[] | .id' downloaded/dimensions.json | tr -d '"' | while read dim; do
    echo $dim
    curl -o downloaded/classifications-$dim.json "https://api.beta.ons.gov.uk/v1/population-types/UR/dimensions/$dim/categorisations"
    # curl -o downloaded/$dim.json "https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations?area-type=nat&dimensions=$dim"
    sleep 0.8
done
