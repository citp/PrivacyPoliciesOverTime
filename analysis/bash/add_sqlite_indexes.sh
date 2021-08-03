#!/bin/bash

export SQLITE_TMPDIR=/n/fs/scratch/rbamos/
for ys in 2016B 2017A 2017B; do #$(./get_all_ys.sh); do
    sqlite3 ../data/sqlite/grams_${ys}.sqlite3 'CREATE INDEX index_nc ON grams(n,count);' &
done

wait
