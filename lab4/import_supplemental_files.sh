#!/bin/bash

BASEDIR="$(cd "`dirname "$0"`" && pwd)"
cd "$BASEDIR"

if [ ! -d ../.git ]; then
    echo "Error: $0 must be run from a subdirectory "
    echo "in your local repository root"
    echo ""
    echo "Hint: did you extracted the supplemental file tarball at your local repository"
    echo "root? If so, there should be a directory labX_supplemental (X is a digit)"
    echo "where you can find echo the src/ directory."
    exit 1
fi

# We are at the repo root now.
cd ..

BASEDIRNAME="`basename "$BASEDIR"`"
NCHANGES=`git status -s | grep -v "?? $BASEDIRNAME" | wc -l`
if [ $? -ne 0 ]; then
    git status
    echo ""
    echo "Error: unable to run git status"
    exit 1
elif [ $NCHANGES -ne 0 ]; then
    git status
    echo ""
    echo "Error: there're uncommitted changes or untracked files."
    echo "Hint: add or remove all untracked files and/or "
    echo "commit or stash all uncommitted changes first."
    exit 1
fi

rollback() {
    echo "Rolling back changes..."
    git checkout -- .
    git clean -fd
    exit 1
}

echo "Copying files..."
for fpath in `find "$BASEDIR" -mindepth 1 \
    -not -name import_supplemental_files.sh -type f -printf "%P\n"`; do
    echo "Copying $fpath"
    dpath="`dirname "$fpath"`" 
    basename="`basename "$fpath"`"
    if [ ! -d "$dpath" ]; then
        mkdir -p "$dpath" || {
            echo "Error: failed to make directory \"$dpath\""
            rollback 
        }
    fi
    cp "$BASEDIR/$fpath" "$dpath/" || {
        echo "Error: failed to copy \"$fpath\""
        rollback
    }
done

cd data || {
    echo "Error: data/ directory not found"
    rollback
}
rm -rf preloaded_btree
tar xf preloaded_btree.tar.xz || {
    echo "Error: unable to extract data/preloaded_tar.xz"
    rollback
}
cd .. # back to repository root
if [ `grep '/data/preloaded_btree' .gitignore | wc -l` -eq 0 ]; then
    echo "/data/preloaded_btree" >> .gitignore
fi

if [ -d include/query ]; then
    rm -rf include/query
fi
if [ -d src/query ]; then
    rm -rf src/query
fi

echo "Removing myself"
rm -rf "$BASEDIR"

git add -A
git status
echo ""
echo "Importing supplemental files finished."
echo "Please commit it and (optional) create a tag."

