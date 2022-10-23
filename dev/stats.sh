#!/bin/bash
fname="./dev/stats/${1%.*}"
root="${2}"

if [[ $# -lt 1 ]]; then
    echo "proper ussage: ./stats.sh filename root_function"
    echo "for function name use Unix/Bash globbing/pattern matching"
    echo "i.e. ./dev/stats.sh model models:*:*"
    echo
    echo "see details: https://github.com/jrfonseca/gprof2dot"
    exit 2
fi

python -m cProfile -o "$fname".profile main.py &&
    gprof2dot -f pstats "$fname".profile -o "$fname".dot -z "$root" &&
    dot -Tpng -o "$fname".png "$fname".dot &&
    eog "$fname".png
