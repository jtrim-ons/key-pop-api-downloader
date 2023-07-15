#!/bin/bash

cat generated/TMP-LTLA-POPS/* > generated/TMP-ALL_LTLA-POPS.txt

#cat generated/TMP-ALL_LTLA-POPS.txt | cut -d' ' -f1 | sort | uniq | while read ltla; do
#    echo $ltla \
#            $(cat generated/TMP-ALL_LTLA-POPS.txt | grep "$ltla" | sort -n | head -n1 | cut -d' ' -f2) \
#            $(cat generated/TMP-ALL_LTLA-POPS.txt | grep "$ltla" | sort -n | tail -n1 | cut -d' ' -f2)
#done | tee generated/TMP-POP-MIN-MAX.txt | nl

cat generated/TMP-POP-MIN-MAX.txt | awk '{$4 = $3/$2; print}' | tee generated/TMP-POP-MIN-MAX-RATIO.txt
