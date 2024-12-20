# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 17:45:58 2024

@author: Administrator
"""

import pandas as pd
import datetime
import re
from sqlalchemy import create_engine
from urllib.parse import quote
raw = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))
tempdb = create_engine("mysql://altrr_db:%s@192.168.0.31:1234/tempdb"% quote("abcd"))

df=pd.read_sql("SELECT Id, DocNo, Seller FROM tbl_maharashtra_igr_basic", raw)

done=pd.read_sql("select Id from tbl_maha_seller_info_correct", tempdb)

done['Id']=done['Id'].apply(lambda x: int(x))

done=done.drop_duplicates(subset='Id')

df_new=df[~df['Id'].isin(done['Id'])]

df1=df_new.copy()

df1['Seller']=df1['Seller'].apply(lambda x:x.replace('\xa0', ' '))
df1['Seller']=df1['Seller'].apply(lambda x:x.replace(' - ', ''))
df1['Seller']=df1['Seller'].apply(lambda x:x.replace('\u200b', ' '))
df1['Seller']=df1['Seller'].apply(lambda x:x.replace('\u200d', ' '))
df1['Seller']=df1['Seller'].apply(lambda x:x.replace(': -', ':-'))
df1['seller_name']=df1['Seller'].apply(lambda x:re.findall(r'नाव:\-(.*?)वय\:',x))
df1['seller_name']=df1['seller_name'].apply(lambda x:x[0] if len(x)>0 else None)
df1['seller_name']=df1['seller_name'].str.replace(' वय','').str.replace('नाव:-','')
df1['seller_name']=df1['seller_name'].str.strip()
df1=df1[~df1['seller_name'].isnull()]
df1['seller_name']=df1['seller_name'].apply(lambda x:x.split(' ') if isinstance(x,str) else x)
df1['seller_name']=df1['seller_name'].apply(lambda x:' '.join(a.replace(' ','') for a in x))

df1['seller_name1']=df1['seller_name'].apply(lambda x:re.findall(r'(मेसर्स|मे\..*?)तर्फे|स्वतः(.*?एन्टरप्राइस\s*)',x))

df1['seller_name1']=df1['seller_name1'].apply(lambda x: str(x))

df1['seller_name1']=df1['seller_name1'].str.replace('[','').str.replace(']','').str.replace(')','').str.replace('(','').str.replace("'",'')

df1['seller_name1']=df1['seller_name1'].apply(lambda x: x.split(', ')[0])       
    
df1['seller_name1']=df1['seller_name1'].str.strip()
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('तर्फे', ''))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('मे.', 'मेसर्स '))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('में.', 'मेसर्स '))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('में ', 'मेसर्स '))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('मे ', 'मेसर्स '))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('-', ''))
df1['seller_name1']=df1['seller_name1'].apply(lambda x:x.replace('--', ''))
df1['seller_name1']=df1['seller_name1'].str.strip()

df1['seller_name1']=df1['seller_name1'].apply(lambda x: None if x=='मेसर्स' else x)
df1['seller_name1']=df1['seller_name1'].apply(lambda x: None if isinstance(x, str) and len(x)<3 else x)
final=df1[~df1['seller_name1'].isnull()]
df1=df1[df1['seller_name1'].isnull()]

patterns=[r"(सेंट्रल\sबँक.*?महाराष्ट्र)",r"(बँक.*?महाराष्ट्र)", r"तर्फे(.*?डेव्हलपर्स)", r"तर्फे(.*?डेव्हलोपर्स)", r"तर्फे(.*?डेव्हलमेंटस)",
r"तर्फे(.*?एल\s?एल\s?पी)", r"तर्फे(.*?एलएलपी)", r"तर्फे(.*?एल\.?एल\.?पी\.?)", r"तर्फे(.*?एल\.?एल\.?\s?पी\.?)",
r"तर्फे(.*?लिमिटेड)", r"तर्फे(.*?\s?लिमी\.?)",
r"तर्फे(.*?\s?ग्रुप्स\s?)", r"तर्फे(.*?\s?ग्रुप\s?)", r"तर्फे(.*?असोसिएटस)", r"तर्फे(.*?रियल्टी)", r"तर्फे(.*?प्रॉपर्टीज)",
"तर्फे(.*?ट्रेडर्स)", r"तर्फे(.*?शेल्टर्स)",  r"तर्फे(.*?इंटरप्राईजेस)", r"तर्फे(.*?बिल्डर्स)",
r"तर्फे(.*?बिल्डकॉन\s?)", r"तर्फे(.*?एन्टरप्राजेस)", r"तर्फे(.*?कॉर्पोरेशन)", "तर्फे(.*?प्रा.लि)", r"तर्फे(.*?संस्था\s?)",
r"तर्फे(.*?इन्फ्रा\s?)", r"तर्फे(.*?व्हेंचर्स)", "तर्फे(.*?इन्व्हेस्टमेंट्स)", r"तर्फे(.*?वेंचर\s?)", r"तर्फे(.*?\s?रिअल्टर्स)",
r"तर्फे(.*?ग्रीनबिल्ड)", r"तर्फे(.*?प्रा\.?\s?लि)", r"तर्फे(.*?कन्स्ट्क्शन्स)", r"तर्फे(.*?एनक्लेव)", r"तर्फे(.*?रिअलटी)",
r"तर्फे(.*?कन्स्ट्रक्शन)", r"तर्फे(.*?कंस्ट्रक्शन)", r"तर्फे(.*?प्रमोटर्स)", r"तर्फे(.*?ईस्टेटस)",
r"तर्फे(.*?असोसिएट्स)", r"तर्फे(.*?चेंबर्स)", r"तर्फे(.*?लाईफस्पेस)", r"तर्फे(.*?कॅपिटल)", r"तर्फे(.*?व्हेंचर्स)",
r"तर्फे(.*?\s?प्रॉपटीज)", r"तर्फे(.*?व्हेंचर्स)", r"तर्फे(.*?लॅण्डमार्कस)", r"तर्फे(.*?इस्टेटस)",
r"तर्फे(.*?मुद्रीका)", r"तर्फे(.*?कंस्ट्रकशन्स)", r"तर्फे(.*?व्हेचर्स)", r"तर्फे(.*?प्रोमोटर्स)", r"तर्फे(.*?इंटरप्राइजेस)",
r"तर्फे(.*?रिअल्टी)", r"तर्फे(.*?इंटरप्रायजेस)", r"तर्फे(.*?व्हेंचर)", r"तर्फे(.*?\s?स्पेस\s?)", r"तर्फे(.*?इंफ्राकॉन)",
r"तर्फे(.*?पिन्नाकल)", r"तर्फे(.*?रिअलटर्स)", r"तर्फे(.*?रिअलकॉन\s?)", r"तर्फे(.*?एम्पीरीअल)"]

final1=pd.DataFrame()
df2=df1.copy()

for pattern in patterns:
    matched=[]
    found_pattern=[]
    for name in df2['seller_name']:
        match=re.search(pattern,str(name))
        if match:
            if len(match[0])>1:
                matched.append(match[0])
                found_pattern.append(pattern)
            else:
                matched.append(None)
                found_pattern.append(None)
        else:
            matched.append(None) 
            found_pattern.append(None)
    df2['seller_name1']=matched
    df2['Pattern']=found_pattern
    found_matches=df2[~df2['seller_name1'].isnull()]
    final1=pd.concat([final1,found_matches], axis=0)
    df2=df2[df2['seller_name1'].isnull()]
    
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split('तर्फे')[-1])
    
patterns=[r"(सेंट्रल\sबँक.*?महाराष्ट्र)",r"(बँक.*?महाराष्ट्र)", r"(.*?डेव्हलपर्स)", r"(.*?डेव्हलोपर्स)", r"(.*?डेव्हलमेंटस)",
r"(.*?एल\s?एल\s?पी)", r"(.*?एलएलपी)", r"(.*?एल\.?एल\.?पी\.?)", r"(.*?एल\.?एल\.?\s?पी\.?)",
r"(.*?लिमिटेड)", r"(.*?\s?लिमी\.?)",r"(.*?\s?ग्रुप्स\s?)", r"(.*?\s?ग्रुप\s?)",
"(.*?ट्रेडर्स)", r"(.*?शेल्टर्स)",  r"(.*?इंटरप्राईजेस)", r"(.*?बिल्डर्स)", r"(.*?असोसिएटस)", r"(.*?रियल्टी)", r"(.*?प्रॉपर्टीज)",
r"(.*?बिल्डकॉन\s?)", r"(.*?एन्टरप्राजेस)", r"(.*?कॉर्पोरेशन)", "(.*?प्रा.लि)", r"(.*?संस्था\s?)",
r"(.*?इन्फ्रा\s?)", r"(.*?व्हेंचर्स)", "(.*?इन्व्हेस्टमेंट्स)", r"(.*?वेंचर\s?)", r"(.*?\s?रिअल्टर्स)",
r"(.*?ग्रीनबिल्ड)", r"(.*?प्रा\.?\s?लि)", r"(.*?कन्स्ट्क्शन्स)", r"(.*?एनक्लेव)", r"(.*?रिअलटी)",
r"(.*?कन्स्ट्रक्शन)", r"(.*?कंस्ट्रक्शन)", r"(.*?प्रमोटर्स)", r"(.*?ईस्टेटस)",
r"(.*?असोसिएट्स)", r"(.*?चेंबर्स)", r"(.*?लाईफस्पेस)", r"(.*?कॅपिटल)", r"(.*?व्हेंचर्स)",
r"(.*?\s?प्रॉपटीज)", r"(.*?संस्थे\s?)", r"(.*?व्हेंचर्स)", r"(.*?लॅण्डमार्कस)", r"(.*?इस्टेटस)",
r"(.*?मुद्रीका)", r"(.*?कंस्ट्रकशन्स)", r"(.*?व्हेचर्स)", r"(.*?प्रोमोटर्स)", r"(.*?इंटरप्राइजेस)",
r"(.*?रिअल्टी)", r"(.*?इंटरप्रायजेस)", r"(.*?व्हेंचर)", r"(.*?इंफ्राकॉन)",
r"(.*?पिन्नाकल)", r"(.*?रिअलटर्स)", r"(.*?रिअलकॉन\s?)", r"(.*?बँक)",
r"(.*?एम्पीरीअल)",r"घेणार(.*?कंपनी\s?)", r"घेणार(.*?डेव्हलपर्स)", r"घेणार(.*?डेव्हलोपर्स)", r"घेणार(.*?डेव्हलमेंटस)",
r"घेणार(.*?एल\s?एल\s?पी)", r"घेणार(.*?एलएलपी)", r"घेणार(.*?एल\.?एल\.?पी\.?)", r"घेणार(.*?एल\.?एल\.?\s?पी\.?)",
r"घेणार(.*?लिमिटेड)", r"घेणार(.*?\s?लि\.?\s?)", r"घेणार(.*?\s?लि\s?)", r"घेणार(.*?\s?लिमी\.?)", r"घेणार(.*?\s?ली.)",
r"घेणार(.*?\s?ग्रुप्स\s?)", r"घेणार(.*?\s?ग्रुप\s?)", r"घेणार(.*?असोसिएटस)", r"घेणार(.*?रियल्टी)", r"घेणार(.*?प्रॉपर्टीज)",
r"घेणार(.*?ट्रेडर्स)", r"घेणार(.*?शेल्टर्स)",  r"घेणार(.*?इंटरप्राईजेस)", r"घेणार(.*?बिल्डर्स)",
r"घेणार(.*?बिल्डकॉन\s?)", r"घेणार(.*?एन्टरप्राजेस)", r"घेणार(.*?कॉर्पोरेशन)", r"घेणार(.*?प्रा\.?लि)", r"घेणार(.*?संस्था\s?)",
r"घेणार(.*?इन्फ्रा\s?)", r"घेणार(.*?व्हेंचर्स)", r"घेणार(.*?इन्व्हेस्टमेंट्स)", r"घेणार(.*?वेंचर\s?)", r"घेणार(.*?\s?रिअल्टर्स)",
r"घेणार(.*?ग्रीनबिल्ड)", r"घेणार(.*?प्रा.\s?लि)", r"घेणार(.*?कन्स्ट्क्शन्स)", r"घेणार(.*?एनक्लेव)", r"घेणार(.*?रिअलटी)",
r"घेणार(.*?कन्स्ट्रक्शन)", r"घेणार(.*?लँडमार्क)", r"घेणार(.*?कंस्ट्रक्शन)", r"घेणार(.*?प्रमोटर्स)", r"घेणार(.*?ईस्टेटस)",
r"घेणार(.*?असोसिएट्स)", r"घेणार(.*?चेंबर्स)", r"घेणार(.*?लाईफस्पेस)", r"घेणार(.*?कॅपिटल)", r"घेणार(.*?व्हेंचर्स)",
r"घेणार(.*?\s?प्रॉपटीज)", r"घेणार(.*?संस्थे\s?)", r"घेणार(.*?व्हेंचर्स)", r"घेणार(.*?लॅण्डमार्कस)", r"घेणार(.*?इस्टेटस)",
r"घेणार(.*?मुद्रीका)", r"घेणार(.*?कंस्ट्रकशन्स)", r"घेणार(.*?व्हेचर्स)", r"घेणार(.*?प्रोमोटर्स)", r"घेणार(.*?इंटरप्राइजेस)",
r"घेणार(.*?रिअल्टी)", r"घेणार(.*?इंटरप्रायजेस)", r"घेणार(.*?व्हेंचर)", r"घेणार(.*?इंफ्राकॉन)",
r"घेणार(.*?पिन्नाकल)", r"घेणार(.*?रिअलटर्स)", r"घेणार(.*?रिअलकॉन\s?)", r"घेणार(.*?इंडिया)", r"घेणार(.*?बँक)",
r"घेणार(.*?एम्पीरीअल)", r"देणार(.*?कंपनी\s?)", r"देणार(.*?डेव्हलपर्स)", r"देणार(.*?डेव्हलोपर्स)", r"देणार(.*?डेव्हलमेंटस)",
r"देणार(.*?एल\s?एल\s?पी)", r"देणार(.*?एलएलपी)", r"देणार(.*?एल\.?एल\.?पी\.?)", r"देणार(.*?एल\.?एल\.?\s?पी\.?)",
r"देणार(.*?लिमिटेड)", r"देणार(.*?\s?लिमी\.?)", r"देणार(.*?\s?ली\.)",
r"देणार(.*?\s?ग्रुप्स\s?)", r"देणार(.*?\s?ग्रुप\s?)", r"देणार(.*?असोसिएटस)", r"देणार(.*?रियल्टी)", r"देणार(.*?प्रॉपर्टीज)",
r"देणार(.*?ट्रेडर्स)", r"देणार(.*?शेल्टर्स)",  r"देणार(.*?इंटरप्राईजेस)", r"देणार(.*?बिल्डर्स)",
r"देणार(.*?बिल्डकॉन\s?)", r"देणार(.*?एन्टरप्राजेस)", r"देणार(.*?कॉर्पोरेशन)", r"देणार(.*?प्रा\.?लि)", r"देणार(.*?संस्था\s?)",
r"देणार(.*?इन्फ्रा\s?)", r"देणार(.*?व्हेंचर्स)", r"देणार(.*?इन्व्हेस्टमेंट्स)", r"देणार(.*?वेंचर\s?)", r"देणार(.*?\s?रिअल्टर्स)",
r"देणार(.*?ग्रीनबिल्ड)", r"देणार(.*?प्रा.\s?लि)", r"देणार(.*?कन्स्ट्क्शन्स)", r"देणार(.*?एनक्लेव)", r"देणार(.*?रिअलटी)",
r"देणार(.*?कन्स्ट्रक्शन)", r"देणार(.*?लँडमार्क)", r"देणार(.*?कंस्ट्रक्शन)", r"देणार(.*?प्रमोटर्स)", r"देणार(.*?ईस्टेटस)",
r"देणार(.*?असोसिएट्स)", r"देणार(.*?चेंबर्स)", r"देणार(.*?लाईफस्पेस)", r"देणार(.*?कॅपिटल)", r"देणार(.*?व्हेंचर्स)",
r"देणार(.*?\s?प्रॉपटीज)", r"देणार(.*?संस्थे\s?)", r"देणार(.*?व्हेंचर्स)", r"देणार(.*?लॅण्डमार्कस)", r"देणार(.*?इस्टेटस)",
r"देणार(.*?मुद्रीका)", r"देणार(.*?कंस्ट्रकशन्स)", r"देणार(.*?व्हेचर्स)", r"देणार(.*?प्रोमोटर्स)", r"देणार(.*?इंटरप्राइजेस)",
r"देणार(.*?रिअल्टी)", r"देणार(.*?इंटरप्रायजेस)", r"देणार(.*?व्हेंचर)", r"देणार(.*?इंफ्राकॉन)",
r"देणार(.*?पिन्नाकल)", r"देणार(.*?रिअलटर्स)", r"देणार(.*?रिअलकॉन\s?)", r"देणार(.*?इंडिया)", r"देणार(.*?बँक)",
 r"देणार(.*?एम्पीरीअल)", r"(.*?\s?लि\.\s?)",r"(.*?कंपनी\s?)", r"(.*?\s?ली\.)", r"(.*?\s?लि\.)"]
    
final2=pd.DataFrame()

for pattern in patterns:
    matched=[]
    found_pattern=[]
    for name in df2['seller_name']:
        match=re.search(pattern,str(name))
        if match:
            if len(match[0])>1:
                matched.append(match[0])
                found_pattern.append(pattern)
            else:
                matched.append(None)
                found_pattern.append(None)
        else:
            matched.append(None) 
            found_pattern.append(None)
    df2['seller_name1']=matched
    df2['Pattern']=found_pattern
    found_matches=df2[~df2['seller_name1'].isnull()]
    final2=pd.concat([final2,found_matches], axis=0)
    df2=df2[df2['seller_name1'].isnull()]
    
final1=pd.concat([final1,final2],axis=0)      

final1['seller_name1']=final1['seller_name1'].apply(lambda x:''.join(x.split('2)')[0]))
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split('म्हणून')[-1])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split(',2)')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split(', 2)')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split(',2.')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split(', 2.')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split(' 2)')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x: x.split('२)')[0])
final1['seller_name1']=final1['seller_name1'].apply(lambda x:''.join(x.split('२')[0]))
final1['seller_name1']=final1['seller_name1'].str.strip()

final=pd.concat([final,final1], axis=0, ignore_index=True)

df2['seller_name1']=df2['seller_name'].apply(lambda x: x.split('तर्फे')[0] if isinstance(x, str) else x)

df2['seller_name1']=df2['seller_name1'].str.replace('लिहून देणार','')

df2['seller_name1']=df2['seller_name1'].str.strip('.,-/')

df2['seller_name1']=df2['seller_name1'].str.strip()

final=pd.concat([final,df2], axis=0, ignore_index=True)
final=final[~final['seller_name1'].isnull()]
# final=final.drop('Pattern', axis=1)
final=final.rename(columns={'seller_name1':'first_seller'})
df=final.drop_duplicates(subset='Id')

df['first_seller']=df['first_seller'].str.replace('\xa0', ' ')
df['first_seller']=df['first_seller'].str.replace('\u200d', ' ')
df['first_seller']=df['first_seller'].str.replace('\u200c',' ')
df['first_seller']=df['first_seller'].str.replace('-', ' ')
df['first_seller']=df['first_seller'].str.strip()

def find_patterns(variable,cell):
    if isinstance(cell,str):
        found_matches=[]
        for pattern in variable:
            match=re.findall(pattern,cell)
            if match:
                found_matches.append(match[0])
                break
            else:
                pass
        final=''.join(found_matches)
        return final
    
def find_patterns1(variable,cell):
    if isinstance(cell,str):
        found_matches=[]
        for pattern in variable:
            match=re.findall(pattern,cell)
            if match:
                cell=match[0]
            else:
                return cell
        found_matches.append(cell)
        final=''.join(found_matches)
        return final
    
age_patterns=[r'वय\:\-?(.*?)पत्ता\:\-?']

df['seller_age']=df['Seller'].apply(lambda x: find_patterns(age_patterns,x))
df['seller_age']=df['seller_age'].str.replace('\xa0', ' ')
df['seller_age']=df['seller_age'].str.strip()
df['seller_age']=df['seller_age'].str.extract(r'([-+]?\d*\.?\d+)')

address_patterns=[r'पत्ता\:\-\s?(.*?)पिन\s?कोड']

df['seller_address']=df['Seller'].apply(lambda x: find_patterns(address_patterns,x))
df['seller_address']=df['seller_address'].str.replace('\xa0', ' ')
df['seller_address']=df['seller_address'].str.strip()

pincode_patterns=['पिन\s?कोड\:?\-?\s?([0-9]{6})']

df['seller_pincode']=df['Seller'].apply(lambda x: find_patterns(pincode_patterns,x))
df['seller_pincode']=df['seller_pincode'].str.replace('\xa0', ' ')
df['seller_pincode']=df['seller_pincode'].str.strip()

pan_patterns=[r'([A-Z]{5}[0-9]{4}[A-Z])']

df['seller_pan_number']=df['Seller'].apply(lambda x: find_patterns(pan_patterns,x))
df['seller_pan_number']=df['seller_pan_number'].str.replace('\xa0', ' ')
df['seller_pan_number']=df['seller_pan_number'].str.strip()

def seller_type(x):
    if isinstance(x,str) and len(x)>3:
        x=x[3]
        if x=='A' or x=='B':
            return 'Association'
        elif x=='C' or x=='F':
            return 'Company'
        elif x=='T':
            return 'Company'
        elif x=='G' or x=='L':
            return 'Government'
        elif x=='J':
            return 'Government'
        elif x=='H' or x=='P':
            return 'Individual'
        else:
            return None
    else:
        pass

df['seller_type']=df['seller_pan_number'].apply(lambda x: seller_type(x))

df['seller_name']=df['seller_name'].str.replace(' - ',' ')

df['CrDt']=datetime.datetime.now()

df=df.map(lambda x:x.strip() if isinstance(x,str) else x)

patterns=["कंपनी", "डेव्हलपर्स", "डेव्हलोपर्स", "डेव्हलमेंटस",
"एलएलपी", "एल.एल.पी.", "लिमिटेड", "बँक ऑफ महाराष्ट्र",
"ग्रुप्स", "ग्रुप", "असोसिएटस", "रियल्टी", "प्रॉपर्टीज",
"ट्रेडर्स", "शेल्टर्स", "इंटरप्राईजेस", "बिल्डर्स",
"बिल्डकॉन", "एन्टरप्राजेस", "कॉर्पोरेशन",
"इन्फ्रा", "इन्व्हेस्टमेंट्स", "रिअल्टर्स",
"ग्रीनबिल्ड", "कन्स्ट्क्शन्स", "एनक्लेव", "रिअलटी",
"कन्स्ट्रक्शन", "लँडमार्क", "कंस्ट्रक्शन", "प्रमोटर्स", "ईस्टेटस",
"असोसिएट्स", "चेंबर्स", "लाईफस्पेस", "कॅपिटल",
"प्रॉपटीज", "संस्थे", "लॅण्डमार्कस", "इस्टेटस",
"मुद्रीका", "कंस्ट्रकशन्स", "प्रोमोटर्स", "इंटरप्राइजेस",
"रिअल्टी", "इंटरप्रायजेस", "व्हेंचर", "स्पेस", "इंफ्राकॉन",
"पिन्नाकल", "रिअलटर्स", "रिअलकॉन", "इंडिया", " बँक ",
"एम्पीरीअल", "लिमिटेड", "ग्रुप्स", "ग्रुप", "असोसिएटस",
 "रियल्टी", "प्रॉपर्टीज", "ट्रेडर्स", "शेल्टर्स", "इंटरप्राईजेस", "बिल्डर्स",
"बिल्डकॉन", "एन्टरप्राजेस", "कॉर्पोरेशन", "संस्था",
"इन्फ्रा", "इन्व्हेस्टमेंट्स", "रिअल्टर्स","ग्रीनबिल्ड", "प्रा.लि",
"कन्स्ट्क्शन्स", "एनक्लेव", "रिअलटी","कन्स्ट्रक्शन", "लँडमार्क",
"कंस्ट्रक्शन", "प्रमोटर्स", "ईस्टेटस",
"असोसिएट्स", "चेंबर्स", "लाईफस्पेस", "कॅपिटल", "व्हेंचर्स",
"प्रॉपटीज", "संस्थे", "लॅण्डमार्कस", "इस्टेटस",
"मुद्रीका", "कंस्ट्रकशन्स", "व्हेचर्स", "प्रोमोटर्स", "इंटरप्राइजेस",
"रिअल्टी", "इंटरप्रायजेस", "व्हेंचर", "स्पेस", "इंफ्राकॉन",
"पिन्नाकल", "रिअलटर्स", "रिअलकॉन", "इंडिया", "एम्पीरीअल",
'मेसर्स']

patterns=list(set(patterns))

individual=df[df['seller_type'].str.contains('Individual',na=True)]

changed=[]
for i in individual['seller_name']:
    if any(word in i for word in patterns):
        changed.append('Company')
    else:
        changed.append('Individual')    
        
individual['seller_type']=changed

df=df[~df['seller_type'].str.contains('Individual',na=True)]

df=pd.concat([df,individual], axis=0)

df=df.drop_duplicates(subset='Id')

df=df.map(lambda x: x.strip() if isinstance(x,str) else x)

df['Id']=df['Id'].apply(lambda x: int(x))

df=df.map(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df['seller_age']=df['seller_age'].apply(lambda x:'0' if x==None else x)

df=df.map(lambda x: None if isinstance(x, str) and x=='None' else x)
df=df.map(lambda x: None if isinstance(x, str) and x=='' else x)
df=df[~df['first_seller'].isnull()]

df.to_excel('seller_new_split_check_14112024.xlsx',index=False)
# df.to_sql('tbl_maha_seller_info_correct', con=engine2, if_exists='append', index=False)



# chunk_size = 1000

# # Create chunks and export to Excel for UI path translation
# for i, start in enumerate(range(0, len(df), chunk_size)):
#     end = min(start + chunk_size, len(df))
#     chunk = df.iloc[start:end]
#     path=r'\\192.168.0.28\datascience\Marathi Translate\Marathi\seller info\\'
#     chunk.to_excel(path+f'chunk {i + 1}.xlsx', index=False)

# print("Data exported to Excel files successfully.") 