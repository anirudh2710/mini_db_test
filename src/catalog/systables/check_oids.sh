#!/bin/bash

if [ "x$1" = "x-h" ]; then
    echo "usage: $0 <init.dat> [print_ranges=false]"
    echo "usage: $0"
    echo "usage: $0 -h"
    exit 1
fi

if [ -n "$1" ]; then
    INITFILE="$1"
    if [ x"$2" = "xtrue" ]; then
        echo "[INFO] Printing existing oid ranges"
        PRINT_OID=1
    else
        PRINT_OID=0
    fi
else
    INITFILE="../../../build/generated_source/catalog/systables/init.dat"
    PRINT_OID=1
fi

echo "Checking whether the oids are unique in init.dat"

awk '
BEGIN{
    oidcol[1]=0
    oidcol[2]=0
    oidcol[4]=0
    oidcol[7]=0
    oidcol[10] = 0
}
{
    if ($1 == "table") {
        tabid = $2
    } else if ($1 == "data") {
        if (tabid in oidcol) {
            c = oidcol[tabid] + 2
            print $c
        }
    } else {
        print "[ERROR] unknown entry in init.dat: $0"
        exit 1
    }
}
' "$INITFILE" | sort -n | uniq -c | awk '
BEGIN{
    has_dup = 0;
    prev_oid_start = 0
    prev_oid_end = 0
    print_oid = '$PRINT_OID'
}
{
    if ($1 > 1) {
        if (has_dup == 0) {
            print "[ERROR] Found duplicate oids:"
            print "[ERROR] count", "oid"
        }
        print $1, $2
        has_dup = 1
    }
    if (print_oid) {
        if (prev_oid_end == 0 || prev_oid_end + 1 == $2) {
            if (prev_oid_start == 0) {
                prev_oid_start = $2
            }
            prev_oid_end = $2 
        } else {
            print "[INFO]", prev_oid_start, "-", prev_oid_end
            prev_oid_start = $2
            prev_oid_end = $2
        }
    }
}
END{
    if (has_dup != 0) {
        exit 1
    }
    if (print_oid) {
        if (prev_oid_start != 0) {
            print "[INFO]", prev_oid_start, "-", prev_oid_end
        }
    }
    exit 0
}'


