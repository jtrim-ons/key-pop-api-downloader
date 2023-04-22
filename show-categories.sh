#!/bin/bash

set -euo pipefail

cat classifications.txt | while read c; do
    echo $c
    filename=$(rg -l $c downloaded/classifications-*.json | head -n1)
    echo $filename
    cat $filename | jq ".items[] | select(.id == \"resident_age_18b\") | .categories[] | .label"
    echo $(cat $filename | jq ".items[] | select(.id == \"resident_age_18b\") | .categories[] | .label" | wc -l) categories.
    echo
done
