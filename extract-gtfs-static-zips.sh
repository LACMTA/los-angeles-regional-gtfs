#!/bin/sh
find ../lacmta/future/ -name '*.zip' -exec sh -c 'unzip -o -d "${1%.*}" "$1"' _ {} \;
echo "Done."