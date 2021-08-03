#!/bin/sh

#USAGE:
#./run_basic_analytics [sample] [clean] [clean-sample-file]
#sample - true/false - Should we use a sample of the data? Default false
#clean - true/false - Should we remove old data files? Default false
#clean-sample-file - true/false - Should we remove the old sample databse?
#                                 WARNING: slow. Default: false


min=3
max=10

cd ../python
mkdir -p ../out/


CLEAN="false"
sample=""
sample_suffix=""
clean_samples="false"

#Process args
if [ "$#" -gt 3 ]; then
    echo "Too many args"
    exit
fi

if [ "$#" -ge 3 ] && $3; then
    clean_samples=true
    echo "Cleaning sample db"
fi


if [ "$#" -ge 2 ] && $2; then
    CLEAN=true
    echo "Cleaning data"
fi

if [ "$#" -ge 1 ] && $1; then
    sample="--use-sample"
    sample_suffix="-sample"
    echo "Using a sample"
fi


#Generate a sample if we need one
if [ ! -z "$sample" ]; then
    if $clean_samples; then
	echo "Removing old sample database"
	rm -f "../data/sqlite/policy-sample.sqlite3"
    fi
    if [ ! -f "../data/sqlite/policy-sample.sqlite3" ]; then
	echo "Making new sample database"
	python -m historical.make_policy_sample
    fi
fi

#Clean the dataset
if true; then
    rm -f ../data/sqlite/clean$sample_suffix.sqlite3
    echo "Cleaning"
    python -m historical.clean $sample
    echo "Done Cleaning"
fi

#Find similar documents
if true; then
    echo "Finding near documents"
    if $CLEAN; then
	rm -f ../data/text_sim/*/*
    fi
    python -m historical.find_near_documents $sample --not-clean all
fi


#Count terms
if true; then
    if $CLEAN; then
	rm -f ../data/sqlite/grams_*$sample_suffix.sqlite3
    fi
    python -m historical.count_terms $sample -swe --start $min --stop $max all;
    echo "Done accumulate counts"
    for f in ../data/sqlite/grams_*.sqlite3; do
	sqlite3 "$f" "CREATE INDEX IF NOT EXISTS index_nc ON grams(n,count);"
    done
    echo "Done creating index"
fi

#Extract terms of interest
if true; then
    echo "Running analytics"
    python -m historical.analytics $sample -swe $min $max 50 > ../out/analytics.txt
    echo "Done with analytics"
fi

if false; then
    echo "Searching for originators"
    for n in s w $(seq 3 10); do
	for metric in diff gain slope gain loss lps lns; do
	    python -m historical.draw_network_phrases $n $metric
	    for thresh in 0 0.1 0.2 0.3 0.4 0.5; do
		python -m historical.find_originators --thresh $thresh $n $metric
		python -m historical.find_originators --randomize 1000 --thresh $thresh $n $metric
	    done
	done
    done
    echo "Done searching for originators"
fi

if false; then
    echo "Searching for originators"
    for randomize in "" "--randomize 1000"; do
	for metric in top gain; do
	    for score in 0 1; do
		for p in "" "--prune"; do
		    for n in $(seq 3 10) s w; do
			echo "python -m historical.find_originators --thresh 0 --score $score $p $randomize $n $metric"
			python -m historical.find_originators --thresh 0 --score $score $p $randomize $n $metric
		    done
		done
	    done
	done
    done
    echo "Done searching for originators"
fi
