#!/bin/bash

make dox >& out.txt
# cat out.txt | grep warning | grep -v "ignoring unsupported tag"
popd
rec="$( grep warning build/out.txt | grep -v "ignoring unsupported tag" |  wc -l )"
echo "$rec"
exit "$rec"

