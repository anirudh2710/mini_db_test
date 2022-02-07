#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: $0 <init.dat>"
    exit 1
fi

echo "Checking whether the oids are unique in init.dat"

awk '
BEGIN{
    oidcol[4]=0
    oidcol[7]=0
    oidcol[1]=0
    oidcol[2]=0
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
        print "unknown entry in init.dat: $0"
        exit 1
    }
}
' "$1" | sort | uniq -c | awk '
BEGIN{
    has_dup = 0;
}
{
    if ($1 > 1) {
        if (has_dup == 0) {
            print "Found duplicate oids:"
            print "count", "oid"
        }
        print $1, $2
        has_dup = 1
    }
}
END{
    if (has_dup != 0) {
        exit 1
    }
    exit 0
}'


