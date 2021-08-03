# Automated analysis software
## Environment setup:
```
#Setup a virtual environment
python3 -m venv venv 
source venv/bin/activate

#Setup submodules
git submodule init && git submodule update
cd simhash-py
git submodule init && git submodule update --recursive
python setup.py install
cd ..

#Install requirements
pip install -r requirements.txt
python -m spacy download "en_core_web_lg"
python -m nltk.downloader stopwords
```

## Data setup:
```
PrivacyPolicyAnalytics/
|--data/
   |--blacklist/
      |--negative_results.pickle
      |--removed_from_v5.pickle
   |--sqlite/
      |--policy.sqlite3
   |--rankings
      |--alexa-top1m-20??-??-??.csv #One for each interval
```

## Running the program
The program can be executed with `run_basic_analytics.sh` in the `bash` directory

Alternately, the pipeline can be run directly, from the `python` directory:

0) (Optional) Run `python -m historical.make_policy_sample` to make a sample set
1) Run `python -m historical.clean` to clean the data
2) Run `python -m historical.find_near_documents all` to find near documents over all intervals
3) Run `python -m historical.count_terms [-s] [-w] [-e] --start [start] --stop [stop] all` to count n-grams over all intervals
4) Run `python -m historical.analytics [-s] [-w] [-e] [start] [stop] 50` to find top n-grams

Add the `--use-sample` flag to use a sample set.
`start` should be the lowest size n-grams desired, `stop` the largest
The `-s` flag counts sentences
The `-e` flag counts named entities, URLs, and emails
The `-w` flag counts case-sensitive unigrams (other n-grams are case insensitive)

# Notebooks
## Running the notebooks
The notebooks can be run with
```
cd notebooks
./run_nb.sh
```

Unfortunately they're a bit of a mess. If you're looking for a specific analysis, I recommend looking for the corresponding graph or number in the pre-generated output.

# Miscellaneous
The `python/` and `python/ncd_graphs/` folders contain scripts from our initial exploration of the which phrases are most "conserved"

The `category_fetch` directory provides tools for downloading WebShrinker and AWIS categories

The `bash` folder has various small utilities

The `public_html` folder is a template for the folder the automated analysis tool will add results to. It contains a search and a document fetch utility. These have hardcoded paths and will need to be modified.
