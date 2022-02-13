#!/bin/sh

CXX=${CXX-c++}
PYTHON3=${PYTHON3-python3}

if [ $# -lt 2 ]; then
    echo "usage: $0 <infile> <out_dir>"
    exit 1
fi

BASEDIR="$(realpath "`dirname "$0"`")"
cd "$BASEDIR"

INFILE="$1"
OUTDIR="$2"
# we also need the generated outdir on our include path so that we can
# directly include the files previously generated
# The two-level above the outdir is where config.h is located
INCLUDE="-I../../../include -I$OUTDIR/../.. -I$OUTDIR"

if [ ! -d "$OUTDIR" ]; then
    mkdir -p "$OUTDIR"
fi

echo '
#include "init_systable_gen.py.inc"

hdr_out_dir = "'"$OUTDIR"'"
src_out_dir = "'"$OUTDIR"'"

#define DEFINE_SYSTABLE(tablename, tableoid, tabledesc) \
TABLENAME, TABLEOID, TABLEDESC = \
STRINGIFY(tablename), \
STRINGIFY(tableoid), \
tabledesc

TABLE_FIELD=[]

#define DEFINE_SYSTABLE_FIELD(sqltype, colname, coldesc) \
    TABLE_FIELD.append(\
    [STRINGIFY(sqltype), sqltype, \
        get_ctype(sqltype), STRINGIFY(colname), coldesc])
#define DEFINE_SYSTABLE_FIELD_OPT(sqltype, colname, defaultval, coldesc) \
    TABLE_FIELD.append(\
    [STRINGIFY(sqltype), sqltype, \
        get_ctype(sqltype), STRINGIFY(colname), coldesc])

#define DEFINE_SYSTABLE_INDEX(...)

#include "'"$INFILE"'"

def parsing_error(file, text, p):
    if p > 10:
        l = p - 10
    else:
        l = 0
    if p <= len(text) - 10:
        r = p + 10
    else:
        r = len(text)
    print("parsing error in {} around {}".format(file, text[l:r]))
    sys.exit(1)

def find(text, pattern, p):
    p2 = text.find(pattern, p)
    if p2 == -1:
        p2 = len(text)
    return p2

def generate_file(srcname, outname):
    with open(srcname) as f:
        hdr = f.read()
    fout = open(outname, "w")
    p = 0
    current_field_idx = -1
    while p < len(hdr):
        p2 = find(hdr, "@", p)
        fout.write(hdr[p:p2])
        if p2 == len(hdr):
            break
        if p2 + 1 >= len(hdr):
            parsing_error(srcname, hdr, p2)
        if hdr[p2 + 1] == "@":
            if current_field_idx >= 0:
                current_field_idx += 1
                if current_field_idx == len(TABLE_FIELD):
                    current_field_idx = -1
                    p = p2 + 2
                else:
                    p = p_save
            else:
                p3 = find(hdr, "@@", p2 + 2)
                if p3 == len(hdr) or hdr[p2:p3+2] != "@@FIELD@@":
                    parsing_error(srcname, hdr, p2)
                current_field_idx = 0
                p = p3 + 2
                p_save = p
        else:
            p3 = find(hdr, "@", p2 + 1)
            if p3 == len(hdr):
                parsing_error(srcname, hdr, p2)
            varname = hdr[p2 + 1: p3]
            if varname == "TABLENAME":
                fout.write(TABLENAME)
            elif varname == "TABLEOID":
                fout.write(TABLEOID)
            elif varname == "TABLEDESC":
                fout.write(TABLEDESC)
            elif varname == "SQLTYPE":
                fout.write(TABLE_FIELD[current_field_idx][0])
            elif varname == "CTYPE":
                fout.write(TABLE_FIELD[current_field_idx][2])
            elif varname == "CTYPE_CREF":
                oid = TABLE_FIELD[current_field_idx][1]
                if oid < 0 or oid == VARCHAR_oid:
                    fout.write("const " + TABLE_FIELD[current_field_idx][2] + "&")
                else:
                    fout.write(TABLE_FIELD[current_field_idx][2])
            elif varname == "COLNAME":
                fout.write(TABLE_FIELD[current_field_idx][3])
            elif varname == "COLDESC":
                fout.write(TABLE_FIELD[current_field_idx][4])
            elif varname == "MOVE_IF_TYPE_NONTRIVIAL":
                oid = TABLE_FIELD[current_field_idx][1]
                if oid < 0 or oid == VARCHAR_oid:
                    fout.write("std::move")
            elif varname == "COMMA_OPT":
                if current_field_idx  + 1 < len(TABLE_FIELD):
                    fout.write(",")
            elif varname == "FIELD_ID":
                fout.write("{}".format(current_field_idx))
            elif varname == "DATUM_GETTER":
                fout.write(get_datum_getter(TABLE_FIELD[current_field_idx][1]))
            elif varname == "DATUM_TRANSFORM":
                oid = TABLE_FIELD[current_field_idx][1]
                if oid < 0:
                    fout.write("NotImplementedVarlenToArray")
                elif oid == VARCHAR_oid:
                    fout.write("VARCHAR_TO_STRING")
            elif varname == "DATUM_CREATOR":
                fout.write(get_datum_creator(TABLE_FIELD[current_field_idx][1]))
            elif varname == "NUM_FIELDS":
                fout.write("{}".format(len(TABLE_FIELD)))
            else:
                print("what?")
                parsing_error(srcname, hdr, p2 + 1)
            p = p3 + 1

    if current_field_idx >= 0:
        parsing_error(srcname, hdr, len(hdr))

    fout.close()

TEMPLATE_HEADER="systable.h.template"
generate_file(TEMPLATE_HEADER, hdr_out_dir + "/" + TABLENAME + ".h")

TEMPLATE_SRC="systable.cpp.template"
generate_file(TEMPLATE_SRC, src_out_dir + "/" + TABLENAME + ".cpp")

' | ${CXX} -E ${INCLUDE} - | ${PYTHON3}

