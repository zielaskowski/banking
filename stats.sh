#!/bin/bash
fname="${1%.*}"
python -m cProfile -o "$fname".profile main.py && gprof2dot -f pstats "$fname".profile | dot -Tpng -o "$fname".png && eog "$fname".png
