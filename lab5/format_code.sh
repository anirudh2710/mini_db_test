#!/bin/bash

# This scripts:
# 1) replaces all tabs with four space characters;
# 2) removes trailing spaces
# in all the source files found in include/, src/ and tests/.


for i in `find include src tests -name '*.cpp' -o -name '*.h' \
    -o -name '*.template' -o -name '*.inc'`; do
    while [ $(grep -P '\t|( $)' $i | wc -l) -gt 0 ]; do
        echo "$i"
        sed -i -e "s/[\t ]$//" -e "s/\t/    /g" $i
    done
done
