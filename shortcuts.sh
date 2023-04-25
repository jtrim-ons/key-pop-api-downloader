o() {open $(ls downloaded/classifications-*$1*json | awk "NR==$2")}
l() {ls -l downloaded/classifications-*$1*json | nl}
c() {echo $1 >> classifications.txt}
