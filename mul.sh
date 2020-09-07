#!/bin/bash

tmpfile=$(mktemp)
cat args.txt > ${tmpfile}
cat ${tmpfile} >> args.txt
rm -f ${tmpfile}
