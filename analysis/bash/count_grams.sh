e="$1"
p="$2"

for ys in $(./get_all_ys.sh); do
    count=$(sqlite3 ../data/sqlite/grams_$ys.sqlite3 "SELECT count FROM grams WHERE n=='$1' and LOWER(phrase)=='$2' LIMIT 1;")
    if [ -z "$count" ]; then
	count="0"
    fi
    echo -n "$p,$count,"
done
echo
