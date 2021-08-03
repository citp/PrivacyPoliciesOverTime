import sqlite3
import csv
import pandas as pd
import bz2
import pickle

db = sqlite3.connect("../data/sqlite/policy.sqlite3")
curs = db.execute("SELECT policy_text, crawl_time FROM policy_texts;")

data = (
    (text,time[:10]) for text,time in curs
    )

df = pd.DataFrame(data=data,columns=["policy_text","date"])

with bz2.open("../data/csv/policy.pkl.bz2","w") as f:
    pickle.dump(df, f)
    
    
