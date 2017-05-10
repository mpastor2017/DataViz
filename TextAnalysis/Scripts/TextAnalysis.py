# -*- coding: utf-8 -*-
"""
Created on Fri May  5 13:36:49 2017

@author: marip
"""

import pandas as pd
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer

# Reading in cleaned version for tf-IDF, subjectivity, polarity
# Reading original text for sentence count
text = pd.read_csv("text.csv")
cleaned_text = pd.read_csv('cleaned_text.csv', sep=',' , encoding='latin-1')
metrics = pd.read_csv("metrics.csv")

# Casting columns to string
for i in list(text):
    text[i] = text[i].astype(str)
for i in list(cleaned_text):
    cleaned_text[i] = cleaned_text[i].astype(str)

# Dropping these
text.drop('host_verifications', axis=1, inplace=True)
text.drop('amenities', axis=1, inplace=True)

# sentiment polarity
for i in list(cleaned_text):
    if i == "V1":
        continue
    else:
        metrics[i + '_polarity'] = cleaned_text[i].apply(lambda x: TextBlob(x).sentiment.polarity)
        
# sentiment subjectivity
for i in list(cleaned_text):
    if i == "V1":
        continue
    else:
        metrics[i + '_subjectivity'] = cleaned_text[i].apply(lambda x: TextBlob(x).sentiment.subjectivity)

# Number of sentences

for i in list(text):
    if i == "id":
        continue
    else:
        metrics[i + '_nb_sent'] = text[i].apply(lambda x: len(TextBlob(x).sentences))
        
# Summed TF-IDF scores for each column
for i in list(cleaned_text):
    if i == 'V1':
        continue
    else:
        # Creating TF-IDF matrix for unigram
        tf = TfidfVectorizer(min_df = 0)
        tfidf_matrix = tf.fit_transform(cleaned_text[i])
        metrics[i + '_tf_idf_sum'] = tfidf_matrix.sum(axis=1).A.squeeze();
     

metrics.to_csv("new_metrics.csv")



