import pickle
import time
import pandas as pd

clf_path = '/u/el24/policyphylog/policy_classifier/classifier.p'
vec_path = '/u/el24/policyphylog/policy_classifier/vectorizers.p'
uids_path = '/u/el24/policyphylog/policy_classifier/ids.pickle'

def main():
	start = time.process_time()
	classifier = pickle.load(open(clf_path, 'rb'))
	vectorizers = pickle.load(open(vec_path, 'rb'))
	df = pd.read_csv('policy_v7.csv', header=None)
	def standardize_text(df, text_field):
		df[text_field] = df[text_field].str.replace(r"http\S+", "")
		df[text_field] = df[text_field].str.replace(r"http", "")
		df[text_field] = df[text_field].str.replace(r"@\S+", "")
		df[text_field] = df[text_field].str.replace(r"[^A-Za-z0-9(),!?@\'\`\"\_\n]", " ")
		df[text_field] = df[text_field].str.replace(r"@", "at")
		df[text_field] = df[text_field].str.replace(r"\n", " ")
		df[text_field] = df[text_field].str.lower()
		return df
	standard_df = standardize_text(df, 7)
	#standard_df = standardize_text(df, "title")
	X_tf = vectorizers['policy_text_vectorizer'].transform(standard_df[7])
	#X_titles = vectorizers['title_vectorizer'].transform(standard_df['title'])
	#import scipy.sparse as sparse
	#X = sparse.hstack((X_tf, X_titles))
	pickle.dump(X_tf, open('X_tf.p', 'wb'))
	#predictions = classifier['model'].predict_proba(X)
	#pickle.dump(predictions, open('predictions.p', 'wb'))
	print("End after %s s" % (time.process_time() - end_uid))

if __name__ == '__main__':
	main()
