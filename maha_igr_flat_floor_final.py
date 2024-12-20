# -*- coding: utf-8 -*-
"""
Created on Sun Oct 20 22:00:19 2024

@author: pc
"""

import pandas as pd
import re
from sqlalchemy import create_engine
from urllib.parse import quote
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

raw = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))
engine_processing = create_engine("mysql://altrr_db:%s@192.168.0.32:3306/processing"% quote("abcd"))

df_main=pd.read_sql("SELECT Id,DocNo,Property_description FROM  altrr_database.tbl_maharashtra_igr_basic", raw)

done=pd.read_sql("select Id from maha_igr_flat_floor_final", engine_processing)

done['Id']=done['Id'].apply(lambda x: int(x))

done=done.drop_duplicates(subset='Id')

df_new=df_main[~df_main['Id'].isin(done['Id'])]

df=df_new.copy()

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df['Id'],df['Property_description']):
    w=w.replace('फ्लॅट',' फ्लॅट ').replace('फ़्लॅट',' फ्लॅट ').replace('रुम',' फ्लॅट ').replace('फ्लाट',' फ्लॅट ').replace('फ्लेट',' फ्लॅट ').replace('फलेट',' फ्लॅट ').replace('फ्लट',' फ्लॅट ').replace('/','-')    
    s=w.split()
    for wr in s:
        if wr == "फ्लॅट":
            ind=s.index("फ्लॅट")
            # print(s.index("युनिट"))
            # print(s[ind:ind+3])
            U=s[ind:ind+5]
            # print(U)
            un=' '.join(U)
            # f=re.findall(r'\b\d+\b',un )
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            # Filter out any standalone numbers that are not unit numbers
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            unit_no.append(f)
            id_.append(I)
            dis.append(w)
            
data0=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})

data0['flat_number_final']=data0['flat_number_final'].str[0]

def floor_no(x):
    data=str(x).strip()
    if data.isdigit() == False:
        return None
    elif len(data)==1:
        return('G')
    elif len(data)==2:
        if (data[0])!=0:
            return None
        else:
            return ('G')
    elif len(data)==3:
        data=data*1
        return(data[0])
    elif len(data)==4:
        data=data*1
        return(data[0:2])
    else:
        return None


data0['floorNo']=data0['flat_number_final'].apply(lambda x: floor_no(x))

def flat_type(cell):
    if 'ऑफिस' in cell:
        return 'C'
    elif 'शॉप' in cell:
        return 'S'
    elif 'दुकान' in cell:
        return 'S'
    elif 'गाळा' in cell:
        return 'S'
    elif 'सदनिका' in cell:
        return 'R'
    elif 'फ्लॅट' in cell:
        return 'R'
    elif 'हवेली' in cell:
        return 'R'
    elif 'घर' in cell:
        return 'R'
    elif 'रुम' in cell:
        return 'R'

    else:
        return None

data0['Flat_type']=data0['Property_description'].apply(lambda x: flat_type(x))

data0.drop('Property_description', axis=1, inplace = True)
data0.drop_duplicates(subset='Id',inplace=True)

df2=df[~df['Id'].isin(data0['Id'])]

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2['Id'],df2['Property_description']):
    w=w.replace('युनिट',' युनिट ').replace('यूनिट',' युनिट ').replace('युनीट',' युनिट ').replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "युनिट":
            ind=s.index("युनिट")
            # print(s.index("युनिट"))
            # print(s[ind:ind+3])
            U=s[ind:ind+5]
            # print(w)
            # print(U)
            un=' '.join(U)
            # print(un)
            # f=re.findall(r'\b\d{3}-\d{4}\b', un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            # Filter out any standalone numbers that are not unit numbers
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            # print(f)
            unit_no.append(f)
            id_.append(I)
            dis.append(w)

data1=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})

data1['flat_number_final']=data1['flat_number_final'].str[0]

data1['floorNo']=data1['flat_number_final'].apply(lambda x: floor_no(x))

data1['Flat_type']=data1['Property_description'].apply(lambda x: flat_type(x))

data1.drop('Property_description', axis=1, inplace = True)

data1.dropna(subset=['flat_number_final'],inplace=True)
data1.drop_duplicates(subset=['Id'],inplace=True)

df2_1=df2[~df2['Id'].isin(data1['Id'])]

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2_1['Id'],df2_1['Property_description']):
    w=w.replace('सदनिका',' सदनिका ').replace('सदनीका',' सदनिका ').replace('स़दनिका',' सदनिका ').replace('सादनिका',' सदनिका ').replace('सदानिका',' सदनिका ').replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "सदनिका":
            ind=s.index("सदनिका")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) >= 1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)
                
data2=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})

data2['flat_number_final']=data2['flat_number_final'].str[0]

data2['floorNo']=data2['flat_number_final'].apply(lambda x: floor_no(x))

data2['Flat_type']=data2['Property_description'].apply(lambda x: flat_type(x))

data2.drop('Property_description', axis=1, inplace = True)

data2.dropna(subset=['flat_number_final'],inplace=True)
data2.drop_duplicates(subset=['Id'],inplace=True)

df2_2=df2_1[~df2_1['Id'].isin(data2['Id'])]

unit_no=[]
id_=[]
dis = []
for I,w in zip(df2_2['Id'],df2_2['Property_description']):
    w=w.replace('अपार्टमेन्ट',' अपार्टमेन्ट ')
    w=w.replace('अपार्टमेण्ट',' अपार्टमेन्ट ')
    w=w.replace('अपार्टमेंट',' अपार्टमेंट ')
    w=w.replace('अपार्टमेंट्स',' अपार्टमेन्ट ')
    w=w.replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "अपार्टमेंट":
            ind=s.index("अपार्टमेंट")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) >=1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)
    
                
data3=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})
data3['flat_number_final']=data3['flat_number_final'].str[0]

data3['floorNo']=data3['flat_number_final'].apply(lambda x: floor_no(x))

data3['Flat_type']=data3['Property_description'].apply(lambda x: flat_type(x))

data3.drop('Property_description', axis=1, inplace = True)

data3.dropna(subset=['flat_number_final'],inplace=True)
data3.drop_duplicates(subset=['Id'],inplace=True)

df2_3=df2_2[~df2_2['Id'].isin(data3['Id'])]

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2_3['Id'],df2_3['Property_description']):
    w=w.replace('शॉप',' शॉप ').replace('दुकान',' शॉप ').replace('शॉप्',' शॉप ').replace('शोप',' शॉप ').replace('शाप',' शॉप ').replace('गाला',' शॉप ').replace('गाळा',' शॉप ').replace('गाळी',' शॉप ').replace('गाल',' शॉप ').replace('दुकाने',' शॉप ').replace('दुकान',' शॉप ').replace('दुकाण',' शॉप ').replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "शॉप":
            ind=s.index("शॉप")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) > 1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)

data4=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})
data4['flat_number_final']=data4['flat_number_final'].str[0]

data4['floorNo']=data4['flat_number_final'].apply(lambda x: floor_no(x))

data4['Flat_type']=data4['Property_description'].apply(lambda x: flat_type(x))

data4.drop("Property_description", axis=1,inplace=True)

data4.dropna(subset=['flat_number_final'],inplace=True)
data4.drop_duplicates(subset=['Id'],inplace=True)

df2_4=df2_3[~df2_3['Id'].isin(data4['Id'])]

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2_4['Id'],df2_4['Property_description']):
    w=w.replace('ऑफिस',' ऑफिस ').replace('कार्यालय',' ऑफिस ').replace('ओफिस',' ऑफिस ').replace('ऑफ़िस',' ऑफिस ').replace('औफिस',' ऑफिस ').replace('आफिस',' ऑफिस ').replace('/','-')     
    s=w.split()
    for wr in s:
        if wr == "ऑफिस":
            ind=s.index("ऑफिस")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) >= 1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)

data5=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})
data5['flat_number_final']=data5['flat_number_final'].str[0]

data5['floorNo']=data5['flat_number_final'].apply(lambda x: floor_no(x))

data5['Flat_type']=data5['Property_description'].apply(lambda x: flat_type(x))

data5.drop("Property_description", axis=1,inplace=True)

data5.dropna(subset=['flat_number_final'],inplace=True)
data5.drop_duplicates(subset=['Id'],inplace=True)

df2_5=df2_4[~df2_4['Id'].isin(data5['Id'])]

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2_5['Id'],df2_5['Property_description']):
    w=w.replace('हवेली',' हवेली ').replace('बंगलो',' हवेली ').replace('घर',' हवेली ').replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "हवेली":
            ind=s.index("हवेली")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) >= 1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)
                
data6=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})
data6['flat_number_final']=data6['flat_number_final'].str[0]

data6['floorNo']=data6['flat_number_final'].apply(lambda x: floor_no(x))

data6['Flat_type']=data6['Property_description'].apply(lambda x: flat_type(x))

data6.drop("Property_description", axis=1,inplace=True)

data6.dropna(subset=['flat_number_final'],inplace=True)
data6.drop_duplicates(subset=['Id'],inplace=True)

df2_6=df2_5[~df2_5['Id'].isin(data6['Id'])] 

unit_no=[]
id_=[]
dis=[]
for I,w in zip(df2_6['Id'],df2_6['Property_description']):
    w=w.replace('हाऊस',' हाऊस ').replace('/','-')
    s=w.split()
    for wr in s:
        if wr == "हाऊस":
            ind=s.index("हाऊस")
            # print(w)
            # print(s[ind:ind+6])
            U=s[ind:ind+6]
            un=' '.join(U)
            # f=re.findall(r'\d+',un)
            unit_numbers = re.findall(r'\b\d{3}-\d{4}\b|\b\d+\b', un)
            f = [num for num in unit_numbers if '-' in num or len(num) >= 2]
            if len(f) >= 1:
                # print(w)
                # print(f)
                unit_no.append(f)
                id_.append(I)
                dis.append(w)

data7=pd.DataFrame({"Id":id_,"Property_description":dis,"flat_number_final":unit_no})
data7['flat_number_final']=data7['flat_number_final'].str[0]

data7['floorNo']=data7['flat_number_final'].apply(lambda x: floor_no(x))

data7['Flat_type']=data7['Property_description'].apply(lambda x: flat_type(x))

data7.drop("Property_description", axis=1,inplace=True)

data7.dropna(subset=['flat_number_final'],inplace=True)
data7.drop_duplicates(subset=['Id'],inplace=True)

final_df1=pd.concat([data0,data1,data2,data3,data4,data5,data6,data7],axis=0) 

final_df1=final_df1.rename(columns={'flat_number_final':'UnitNo','floorNo':'FloorNo','Flat_type':'BuildingType'})

final_df1['tbID']=final_df1['Id'].apply(lambda x: 'MH'+str(x))

final_df1.to_sql('maha_igr_flat_floor_final',engine_processing,index=False,if_exists='append')  
