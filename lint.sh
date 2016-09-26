#!/bin/bash
set -o errexit

find query_tcga test -name '*.py' \
    | xargs pylint \
            --errors-only \
            --disable=print-statement

echo 'Passes pylint check'
