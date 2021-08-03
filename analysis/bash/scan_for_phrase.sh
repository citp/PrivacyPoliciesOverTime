phrase="$1"
tags="$2"
norm_params=($3)
count_override="$4"
if [ -z "$count_override" ]; then
    count=$(echo "$phrase" | wc -w)
else
    count="$count_override"
fi
first=true
echo -n '"'"$phrase"'"'
i=0
for year in $(seq 2009 2019); do
    for season in A B; do
	ct=$(grep -m 1 "$phrase" ../data/$year$season/${count}grams${tags}.csv | cut -c 1-160 | sort | cut -d, -f1)
	echo "grep -m 1 \"$phrase\" ../data/$year$season/${count}grams${tags}.csv | cut -c 1-160 | sort | cut -d, -f1" >&2
	echo "$ct" >&2
	ct=$(echo "scale=1;"0$ct'*'${norm_params[$i]}"/1" | bc)
	echo -n ","
	echo -n "$ct"
	i=$((i+1))	
    done
done
echo
