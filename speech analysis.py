# -*- coding: utf-8 -*-
"""
Created on Thu Jan  4 11:22:55 2024

@author: Admin
"""

import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import pandas as pd
import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
import re

service = Service()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
driver.get("https://rbi.org.in/Scripts/BS_SpeechesView.aspx?Id=1069")
driver.maximize_window()
time.sleep(5)

date=driver.find_element(By.XPATH, '//*[@id="doublescroll"]/table[2]/tbody/tr[2]/td')
curr_date=date.text.split(': ')[1]

speech_title=driver.find_element(By.XPATH, '//*[@id="doublescroll"]/table[2]/tbody/tr[3]/td')

all_paras=driver.find_elements(By.XPATH,'//*[@class="td"]//p')

content=[]
indexed=[]
i=0
for each_para in all_paras:
        content.append(each_para.text)
        i+=1
        indexed.append(i)
            
df=pd.DataFrame()
df['content']=content
df['para_id']=indexed
df.dropna(inplace=True)

sentences=[]
sentenceid=[]
para_id_=[]
final_sent_id=[]
pub_date=[]
speech_head=[]

for i, row in df.iterrows():
    paras=row['content']
    paraid=row['para_id']
    tokenized_sentences=nltk.sent_tokenize(paras)
    indexed=enumerate(tokenized_sentences, start=1)
    sentences.extend(tokenized_sentences)
    sentenceid.extend(indexed)
    for each_Sentence in tokenized_sentences:
        para_id_.append(paraid)
        pub_date.append(curr_date)
        speech_head.append(speech_title.text)
        
        
final_index=[]      
for each_index in sentenceid:
    final_index.append(list(each_index))
    
for each_sent in final_index:
    final_sent_id.append(each_sent[0])

df1=pd.DataFrame()
df1['publish date']=pub_date
df1['speech title']=speech_head
df1['paraid']=para_id_
df1['sentenceid']=final_sent_id
df1['sentences']=sentences

stop_words=stopwords.words('English')
def remove_stopwords(sentence):
    words=nltk.word_tokenize(str(sentence))
    filtered_words=[word for word in words if word.lower() not in stop_words]
    return " ".join(filtered_words)

df1['sent without sw']=df1['sentences'].apply(remove_stopwords)

driver.close()

dict_words=pd.read_excel(r"C:\Users\Admin\Documents\Prashant\speech analysis\speech analysis keywords.xlsx")

mont_conditions_filter="|".join(dict_words["Monetary conditions"].dropna().tolist())

fin_stab_filter="|".join(dict_words["Financial stability"].dropna().tolist())

external_competitiveness_filter="|".join(dict_words["External competitiveness"].dropna().tolist())

economic_growth_filter="|".join(dict_words["Economic growth"].dropna().tolist())

labour_issues_filter="|".join(dict_words["Labour and social issues"].dropna().tolist())


def pattern_searcher(search_sen:str, search_word:str):
    search_result=re.findall(str(search_word), str(search_sen))
    return_str=''
    if len(search_result)>=1:
        for i in search_result:
            return_str+=" "+i
    else:
        return_str=None
    return return_str    
        
df1['monetary conditions']=df1['sent without sw'].apply(lambda x: pattern_searcher(search_sen=x, search_word=mont_conditions_filter))

df1['Financial stability']=df1['sent without sw'].apply(lambda x:pattern_searcher(search_sen=x, search_word=fin_stab_filter))

df1['External competitiveness']=df1['sent without sw'].apply(lambda x:pattern_searcher(search_sen=x, search_word=external_competitiveness_filter))

df1['Economic growth']=df1['sent without sw'].apply(lambda x:pattern_searcher(search_sen=x, search_word=economic_growth_filter))

df1['Labour and social issues']=df1['sent without sw'].apply(lambda x:pattern_searcher(search_sen=x, search_word=labour_issues_filter))

# df1.to_excel("sample.xlsx")

def count_searcher(search_sen:str, search_word:str):
    search_result=re.findall(str(search_word), str(search_sen))
    return_count=0
    if len(search_result)>=1:
        for i in search_result:
            return_count+=1
    else:
        return_count=None
    return return_count

df1['monetary conditions count']=df1['sent without sw'].apply(lambda x: count_searcher(search_sen=x, search_word=mont_conditions_filter))

df1['Financial stability count']=df1['sent without sw'].apply(lambda x:count_searcher(search_sen=x, search_word=fin_stab_filter))

df1['External competitiveness count']=df1['sent without sw'].apply(lambda x:count_searcher(search_sen=x, search_word=external_competitiveness_filter))

df1['Economic growth count']=df1['sent without sw'].apply(lambda x:count_searcher(search_sen=x, search_word=economic_growth_filter))

df1['Labour and social issues count']=df1['sent without sw'].apply(lambda x:count_searcher(search_sen=x, search_word=labour_issues_filter))

df1.to_excel(r"C:\Users\Admin\Documents\Prashant\speech analysis\data\Some Reflections on Micro Credit and How a Public Credit Registry Can Strengthen It jan24_2019.xlsx", index=False)








