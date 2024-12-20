# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 17:48:33 2024

@author: Administrator
"""

import pandas as pd
from sqlalchemy import *
from urllib.parse import quote
import urllib3
import unicodedata
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
engine_fdb = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/fdb"% quote("abcd"))
engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))

query='select * from projectFtbl'

df=pd.read_sql(query,engine_fdb)

find=df[['projectReraId', 'projectName', 'projectVillage', 'street', 'locality', 'pincode', 'city', 'state', 'taluka']]

find.drop_duplicates(subset='projectReraId',inplace=True)

find=find[~find['projectReraId'].isnull()]

find=find[find['state']=='Maharashtra']

find=find.map(lambda x:None if isinstance(x,str) and len(x)==1 else x)

all_filled=find[~find[['projectName', 'projectVillage', 'street', 'locality', 'taluka']].isnull().any(axis=1)]

df1=pd.read_sql('select * from tbl_maharashtra_property_description_translated',engine_processing)

backup=df1.copy()

backup1=all_filled.copy()

df1=df1.map(lambda x: x.lower() if isinstance(x,str) else x)

df1['tbID']=df1['Id'].apply(lambda x: 'MH'+str(x))

done=pd.read_sql('maha_rera_match_final', con=engine_processing)

df1=df1[~df1['tbID'].isin(done['tbID'])]

df1=df1.drop('Id',axis=1)

df1=df1[0:100000]

final=pd.DataFrame()

import warnings
import re
warnings.filterwarnings("ignore")

all_filled['projectName']=all_filled['projectName'].str.lower()

all_filled['projectVillage']=all_filled['projectVillage'].str.lower()

all_filled['street']=all_filled['street'].str.lower()

all_filled['locality']=all_filled['locality'].str.lower()

for k,p,v,s,l,t in zip(all_filled['projectReraId'],all_filled['projectName'],all_filled['projectVillage'],all_filled['street'],all_filled['locality'],all_filled['taluka']):
    p = " " + p
    if len(df1[
        df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
        df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
        df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
        df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
        df1['Property_description'].str.contains(re.escape(t), case=False, na=False)
    ]) > 0:
        match = df1[
            df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
            df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
            df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
            df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
            df1['Property_description'].str.contains(re.escape(t), case=False, na=False)
        ]
        match['key'] = p
        match['Rera'] = k
        match['match_count'] = 5
        print(match)
        final = pd.concat([final, match])
        df1.drop(match.index, inplace=True)
        try:
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)
        except:
            engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)

    # Check for any 4 fields (p + any three of v, s, l, t)
    elif len(df1[
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
    ]) > 0:
        match = df1[
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
        ]
        match['key'] = p
        match['Rera'] = k
        match['match_count'] = 4
        print(match)
        final = pd.concat([final, match])
        df1.drop(match.index, inplace=True)
        try:
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)
        except:
            engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)

    # Check for any 3 fields (p + any two of v, s, l, t)
    elif len(df1[
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
    ]) > 0:
        match = df1[
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
        ]
        match['key'] = p
        match['Rera'] = k
        match['match_count'] = 3
        print(match)
        final = pd.concat([final, match])
        df1.drop(match.index, inplace=True)
        try:
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)
        except:
            engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)

    # Check for any 2 fields (p + any one of v, s, l, t)
    elif len(df1[
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(v), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(s), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
        (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
         df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
    ]) > 0:
        match = df1[
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(v), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(s), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(l), case=False, na=False)) |
            (df1['Property_description'].str.contains(re.escape(p), case=False, na=False) &
             df1['Property_description'].str.contains(re.escape(t), case=False, na=False))
        ]
        match['key'] = p
        match['Rera'] = k
        match['match_count'] = 2
        print(match)
        final = pd.concat([final, match])
        df1.drop(match.index, inplace=True)
        try:
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)
        except:
            engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)

    # Check for only p (1 field match)
    elif len(df1[
        df1['Property_description'].str.contains(re.escape(p), case=False, na=False)
    ]) > 0:
        match = df1[
            df1['Property_description'].str.contains(re.escape(p), case=False, na=False)
        ]
        match['key'] = p
        match['Rera'] = k
        match['match_count'] = 1
        df1.drop(match.index, inplace=True)
        print(match)
        final = pd.concat([final, match])
        try:
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)
        except:
            engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing" % quote("abcd"))
            match.to_sql('maha_rera_match_final', engine_processing, if_exists='append', index=False)

    else:
        pass
