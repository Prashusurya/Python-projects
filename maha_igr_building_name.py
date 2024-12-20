# -*- coding: utf-8 -*-
"""
Created on Mon Oct 14 10:25:19 2024

@author: Administrator
"""

import pandas as pd
import re
from sqlalchemy import create_engine
from urllib.parse import quote
import warnings
warnings.filterwarnings("ignore")
raw = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))
engine2 = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing"% quote("abcd"))

df=pd.read_sql("SELECT Id,DocNo,Property_description FROM  altrr_database.tbl_maharashtra_igr_basic", raw)

done=pd.read_sql("select Id from tbl_Maha_Final_Building_Name", engine2)

df_new=df[~df['Id'].isin(done['Id'])]

df1=df_new.copy()

def reduce_noise(dataframe):
    dataframe['building_name']=dataframe['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
    dataframe['building_name']=dataframe['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
    dataframe['building_name']=dataframe['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
    dataframe['building_name']=dataframe['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)
    dataframe['building_name']=dataframe['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)

df1['Property_description']=df1['Property_description'].apply(lambda x:x.replace('\xa0', ' '))
df1['Property_description']=df1['Property_description'].apply(lambda x:x.replace(' - ', ''))
df1['Property_description']=df1['Property_description'].apply(lambda x:x.replace('\u200b', ' '))
df1['Property_description']=df1['Property_description'].apply(lambda x:x.replace('\u200d', ' '))

df1=df1[['Id', 'DocNo', 'Property_description']]

df1['building_name']=df1['Property_description'].apply(lambda x: '|'.join(re.findall(r'इमारतीचे\s?नाव\:([^\:]+)',x)))

df1['building_name']=df1['building_name'].str.replace(', ब्लॉक नं','')
df1['building_name']=df1['building_name'].str.strip()
reduce_noise(df1)

df2=df1[df1['building_name'].isnull()]

df1=df1[~df1['building_name'].isnull()]

df2['building_name']=df2['Property_description'].apply(lambda x: '|'.join(re.findall(r'आलेल्या(.*?)मधील',x)))

df2['building_name']=df2['building_name'].apply(lambda x: x.split(', ')[0])
df2['building_name']=df2['building_name'].str.strip()
df2['building_name']=df2['building_name'].apply(lambda x: x.split(' या ')[0] if isinstance(x,str) else x)
reduce_noise(df2)

df3=df2[df2['building_name'].isnull()]

df2=df2[~df2['building_name'].isnull()]

df3['building_name']=df3['Property_description'].apply(lambda x: '|'.join(re.findall(r'\"[^\"]+',x)))

df3['building_name']=df3['building_name'].apply(lambda x: x.split(', ')[0])
df3['building_name']=df3['building_name'].apply(lambda x: x.split(' या ')[0])
df3['building_name']=df3['building_name'].apply(lambda x: x.split(',')[0])
df3['building_name']=df3['building_name'].str.replace('"','').str.strip() 
df3['building_name']=df3['building_name'].apply(lambda x: x.split('”')[0] if '”' in x else x)
df3['building_name']=df3['building_name'].apply(lambda x: x.split("''")[0] if "''" in x else x)

def check_number(val):
    try:
        float_val = float(val)
        if str(float_val) == val or str(int(float_val)) == val:
            return None
        else:
            return val
    except ValueError:
        return val

df3['building_name']=df3['building_name'].apply(check_number)
reduce_noise(df3)

df4=df3[df3['building_name'].isnull()]

df3=df3[~df3['building_name'].isnull()]

df4['building_name']=df4['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधलेल्या(.*?)या\s',x)))

df4['building_name']=df4['building_name'].apply(lambda x: x.split(',')[0])
df4['building_name']=df4['building_name'].apply(lambda x: x.split('मधील')[0])
df4['building_name']=df4['building_name'].str.strip()
df4['building_name'] =df4['building_name'].apply(lambda x:check_number(x) if isinstance(x,str) else x)
reduce_noise(df4)

df5=df4[df4['building_name'].isnull()]

df4=df4[~df4['building_name'].isnull()]

df5['building_name']=df5['Property_description'].apply(lambda x: '|'.join(re.findall(r'मिळकतीवरील(.*?)मधील',x)))

df5['building_name']=df5['building_name'].apply(lambda x: x.split(',')[0])
df5['building_name']=df5['building_name'].apply(lambda x: x.split(' या ')[0])
df5['building_name']=df5['building_name'].str.strip()
df5['building_name'] =df5['building_name'].apply(lambda x:check_number(x) if isinstance(x,str) else x)
reduce_noise(df5)

df6=df5[df5['building_name'].isnull()]

df5=df5[~df5['building_name'].isnull()]

df6['building_name']=df6['Property_description'].apply(lambda x:'|'.join(re.findall(r'“(.*?)”',x)))

df6['building_name']=df6['building_name'].apply(lambda x:x.split(',')[0])
df6['building_name']=df6['building_name'].str.strip()
df6['building_name']=df6['building_name'].apply(lambda x:None if isinstance(x,str) and 'केलेल्या' in x else x)
df6['building_name']=df6['building_name'].apply(lambda x:None if isinstance(x,str) and 'असलेल्या' in x else x)
reduce_noise(df6)

df7=df6[df6['building_name'].isnull()]

df6=df6[~df6['building_name'].isnull()]

df7['building_name']=df7['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधण्यात(.*?)मधील',x)))

df7['building_name']=df7['building_name'].apply(lambda x: x.split('येणा-या')[-1] if 'येणा-या' in x else x)
df7['building_name']=df7['building_name'].apply(lambda x: x.split('येणाऱ्या')[-1] if 'येणाऱ्या' in x else x)
df7['building_name']=df7['building_name'].apply(lambda x: x.split('आलेली')[-1] if 'आलेली' in x else x)
df7['building_name']=df7['building_name'].apply(lambda x: x.split(' या ')[0] if ' या ' in x else x) 
df7['building_name']=df7['building_name'].apply(lambda x: x.split('आलेले')[-1] if 'आलेले' in x else x)
df7['building_name']=df7['building_name'].apply(lambda x: x.split('येत असलेल्या ')[-1] if 'येत असलेल्या ' in x else x)
df7['building_name']=df7['building_name'].str.strip()
df7['building_name']=df7['building_name'].apply(lambda x: x.split(',')[0])
reduce_noise(df7)

df8=df7[df7['building_name'].isnull()]

df7=df7[~df7['building_name'].isnull()]

df8['building_name']=df8['Property_description'].apply(lambda x: '|'.join(re.findall(r'येणा-या(.*?)मधील',x)))

df8['building_name']=df8['building_name'].apply(lambda x: x.split(' या ')[0])
df8['building_name']=df8['building_name'].apply(lambda x: x.split(',')[0])
df8['building_name']=df8['building_name'].str.replace('इमारती','').str.strip()
df8['building_name']=df8['building_name'].apply(lambda x:None if 'चौ.' in x else x)
df8['building_name']=df8['building_name'].apply(lambda x:None if isinstance(x,str) and 'क्षेत्रा' in x else x)
df8['building_name']=df8['building_name'].apply(lambda x:None if isinstance(x,str) and 'जमीनीचे' in x else x)
reduce_noise(df8)

df9=df8[df8['building_name'].isnull()]

df8=df8[~df8['building_name'].isnull()]

df9['building_name']=df9['Property_description'].apply(lambda x: '|'.join(re.findall(r'(.{80})प्रोजेक्ट\sमधील',x)))

df9['building_name']=df9['building_name'].apply(lambda x: x.split(',')[-1] if ',' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('मधील')[-1] if 'मधील' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('असलेल्या')[-1] if 'असलेल्या' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('यावरील')[-1] if 'यावरील' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('या वरील')[-1] if 'या वरील' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split(' वरील ')[-1] if ' वरील ' in x else x)
df9['building_name']=df9['building_name'].str.strip()
df9['building_name']=df9['building_name'].apply(lambda x:x.split(' '))
df9['building_name']=df9['building_name'].apply(lambda x:' '.join(x[0:-1]) if x[-1]=='या' else ' '.join(x[0:]))
df9['building_name']=df9['building_name'].apply(lambda x: x.split('अलेल्या')[-1] if 'अलेल्या' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('मिळकतीवर')[-1] if 'मिळकतीवर' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('येथील')[-1] if 'येथील' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('जागेवरील')[-1] if 'जागेवरील' in x else x)
df9['building_name']=df9['building_name'].apply(lambda x: x.split('केलेल्या')[-1] if 'केलेल्या' in x else x)
df9['building_name']=df9['building_name'].str.strip()
reduce_noise(df9)

df10=df9[df9['building_name'].isnull()]

df9=df9[~df9['building_name'].isnull()]

df10['building_name']=df10['Property_description'].apply(lambda x: '|'.join(re.findall(r',(.*?लिमिटेड)',x)))

df10['building_name']=df10['building_name'].apply(lambda x: x.split(',')[-1])
df10['building_name']=df10['building_name'].str.strip()
df10['building_name']=df10['building_name'].apply(lambda x: x.split('मधील')[-1] if isinstance(x,str) and 'मधील' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('असलेल्या')[-1] if isinstance(x,str) and 'असलेल्या' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('केलेल्या')[-1] if isinstance(x,str) and 'केलेल्या' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('वरील')[-1] if isinstance(x,str) and 'वरील' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('बांधलेल्या')[-1] if isinstance(x,str) and 'बांधलेल्या' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('आलेल्या')[-1] if isinstance(x,str) and 'आलेल्या' in x else x)
df10['building_name']=df10['building_name'].apply(lambda x: x.split('असलेली')[-1] if isinstance(x,str) and 'असलेली' in x else x)
reduce_noise(df10)

df11=df10[df10['building_name'].isnull()]
df10=df10[~df10['building_name'].isnull()]

df11['building_name']=df11['Property_description'].apply(lambda x: '|'.join(re.findall(r',(.*?लि\.)',x)))

df11['building_name']=df11['building_name'].apply(lambda x: x.split(',')[-1])
df11['building_name']=df11['building_name'].str.strip()
df11['building_name']=df11['building_name'].apply(lambda x: x.split('मधील')[-1] if isinstance(x,str) and 'मधील' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('असलेल्या')[-1] if isinstance(x,str) and 'असलेल्या' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('केलेल्या')[-1] if isinstance(x,str) and 'केलेल्या' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('वरील')[-1] if isinstance(x,str) and 'वरील' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('बांधलेल्या')[-1] if isinstance(x,str) and 'बांधलेल्या' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('आलेल्या')[-1] if isinstance(x,str) and 'आलेल्या' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('असलेली')[-1] if isinstance(x,str) and 'असलेली' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('आलेली')[-1] if isinstance(x,str) and 'आलेली' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('मिळकतीवर')[-1] if isinstance(x,str) and 'मिळकतीवर' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('बांधलेली')[-1] if isinstance(x,str) and 'बांधलेली' in x else x)
df11['building_name']=df11['building_name'].apply(lambda x: x.split('मध्ये')[-1] if isinstance(x,str) and 'मध्ये' in x else x)
reduce_noise(df11)

df12=df11[df11['building_name'].isnull()]

df11=df11[~df11['building_name'].isnull()]

df12['building_name']=df12['Property_description'].apply(lambda x: '|'.join(re.findall(r'‘‘[^’’]+',x)))

df12['building_name']=df12['building_name'].apply(lambda x:x.split(',')[0])
df12['building_name']=df12['building_name'].apply(lambda x:x.split('”')[0])
df12['building_name']=df12['building_name'].apply(lambda x:x.split(' या ')[0])
df12['building_name']=df12['building_name'].apply(lambda x:None if 'लिहून' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and 'चौ.' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and 'आकार ' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and 'रु.' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and 'हिस्सा' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and 'दस्ताने' in x else x)
df12['building_name']=df12['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df12['building_name']=df12['building_name'].str.replace('‘‘','').str.strip()
reduce_noise(df12)

df13=df12[df12['building_name'].isnull()]

df12=df12[~df12['building_name'].isnull()]

df13['building_name']=df13['Property_description'].apply(lambda x: '|'.join(re.findall(r',(.*?लिमीटेड)',x)))

df13['building_name']=df13['building_name'].apply(lambda x: x.split(',')[-1])
df13['building_name']=df13['building_name'].str.strip()
df13['building_name']=df13['building_name'].apply(lambda x: x.split('मधील')[-1] if isinstance(x,str) and 'मधील' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('असलेल्या')[-1] if isinstance(x,str) and 'असलेल्या' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('केलेल्या')[-1] if isinstance(x,str) and 'केलेल्या' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('वरील')[-1] if isinstance(x,str) and 'वरील' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('बांधलेल्या')[-1] if isinstance(x,str) and 'बांधलेल्या' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('आलेल्या')[-1] if isinstance(x,str) and 'आलेल्या' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x: x.split('असलेली')[-1] if isinstance(x,str) and 'असलेली' in x else x)
df13['building_name']=df13['building_name'].apply(lambda x:None if isinstance(x,str) and ' लिहून ' in x else x)
df13['building_name']=df13['building_name'].str.strip()
reduce_noise(df13)

df14=df13[df13['building_name'].isnull()]

df13=df13[~df13['building_name'].isnull()]

df14['building_name']=df14['Property_description'].apply(lambda x: '|'.join(re.findall(r',(.*?सोसा\sलि\s)',x)))

df14['building_name']=df14['building_name'].apply(lambda x: x.split(',')[-1])
df14['building_name']=df14['building_name'].str.strip()

df14['building_name']=df14['building_name'].apply(lambda x: x.split('मधील')[-1] if isinstance(x,str) and 'मधील' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('असलेल्या')[-1] if isinstance(x,str) and 'असलेल्या' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('केलेल्या')[-1] if isinstance(x,str) and 'केलेल्या' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('वरील')[-1] if isinstance(x,str) and 'वरील' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('बांधलेल्या')[-1] if isinstance(x,str) and 'बांधलेल्या' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('आलेल्या')[-1] if isinstance(x,str) and 'आलेल्या' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('असलेली')[-1] if isinstance(x,str) and 'असलेली' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('मध्ये')[-1] if isinstance(x,str) and 'मध्ये' in x else x)
df14['building_name']=df14['building_name'].apply(lambda x: x.split('नं ')[-1] if isinstance(x,str) and 'नं ' in x else x)
reduce_noise(df14)

df15=df14[df14['building_name'].isnull()]

df14=df14[~df14['building_name'].isnull()]

df15['building_name']=df15['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधण्यात\sआलेल्या(.*?)\sया\s',x)))

df15['building_name']=df15['building_name'].str.strip()
df15['building_name']=df15['building_name'].apply(lambda x:None if isinstance(x,str) and "चौ फुट" in x else x)
df15['building_name']=df15['building_name'].apply(lambda x:x.split('तिस-या')[0] if isinstance(x,str) else x)
df15['building_name']=df15['building_name'].apply(lambda x:x.split('सी विंग')[0] if isinstance(x,str) else x)
reduce_noise(df15)

df16=df15[df15['building_name'].isnull()]

df15=df15[~df15['building_name'].isnull()]

df16['building_name']=df16['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधलेली\s(.*?)मधील',x)))

df16['building_name']=df16['building_name'].apply(lambda x:x.split(' या ')[0])
df16['building_name']=df16['building_name'].apply(lambda x:x.split(',')[0])
df16['building_name']=df16['building_name'].str.strip()
df16['building_name']=df16['building_name'].apply(lambda x: None if isinstance(x,str) and 'यासी' in x else x)
reduce_noise(df16)

df17=df16[df16['building_name'].isnull()]

df16=df16[~df16['building_name'].isnull()]

df17['building_name']=df17['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधकाम\sकेलेल्या\s(.*?)मधील',x)))

df17['building_name']=df17['building_name'].str.strip()
df17['building_name']=df17['building_name'].apply(lambda x: x.split(',')[0])
df17['building_name']=df17['building_name'].apply(lambda x: x.split('मजल्या')[0] if isinstance(x,str) else x)
df17['building_name']=df17['building_name'].str.replace(' या','')
df17['building_name']=df17['building_name'].apply(lambda x:None if isinstance(x,str) and 'मजल्या' in x else x)
reduce_noise(df17)

df18=df17[df17['building_name'].isnull()]

df17=df17[~df17['building_name'].isnull()]

df18['building_name']=df18['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधकाम\sकेलेल्या\s(.*?)\sया\s',x)))

df18['building_name']=df18['building_name'].apply(lambda x: x.split('या नावाने')[0] if isinstance(x,str) else x)
df18['building_name']=df18['building_name'].str.replace('आणि ','')
reduce_noise(df18)


df19=df18[df18['building_name'].isnull()]

df18=df18[~df18['building_name'].isnull()]

df19['building_name']=df19['Property_description'].apply(lambda x: '|'.join(re.findall(r'मिळकतीवर\sबांधलेल्या(.*?)मधील',x)))

df19['building_name']=df19['building_name'].str.strip()
df19['building_name']=df19['building_name'].apply(lambda x: None if 'चौ. फूट' in x else x)
reduce_noise(df19)

df20=df19[df19['building_name'].isnull()]

df19=df19[~df19['building_name'].isnull()]

df20['building_name']=df20['Property_description'].apply(lambda x: '|'.join(re.findall(r'मिळकतीवर\sबांधलेल्या(.*?)मधिल',x)))
df20['building_name']=df20['building_name'].str.strip()
df20['building_name']=df20['building_name'].apply(lambda x:None if x=='इमारती' else x)
reduce_noise(df20)

df21=df20[df20['building_name'].isnull()]

df20=df20[~df20['building_name'].isnull()]

df21['building_name']=df21['Property_description'].apply(lambda x: '|'.join(re.findall(r'\sयावरील(.*?)मधील',x)))

df21['building_name']=df21['building_name'].apply(lambda x:x.split('(नोंदणी क्रं.')[0] if isinstance(x,str) else x)
df21['building_name']=df21['building_name'].str.strip()
reduce_noise(df21)

df22=df21[df21['building_name'].isnull()]

df21=df21[~df21['building_name'].isnull()]

df22['building_name']=df22['Property_description'].apply(lambda x: '|'.join(re.findall(r'\sयावरील(.*?)मधिल',x)))

df22['building_name']=df22['building_name'].apply(lambda x: x.split('योणाऱ्या')[-1] if isinstance(x,str) else x)
df22['building_name']=df22['building_name'].str.strip()
df22['building_name']=df22['building_name'].apply(lambda x:None if isinstance(x,str) and 'प्लॉट' in x else x)
reduce_noise(df22)


df23=df22[df22['building_name'].isnull()]

df22=df22[~df22['building_name'].isnull()]

df23['building_name']=df23['Property_description'].apply(lambda x: '|'.join(re.findall(r'\sवरील(.*?)इमारतीमधील',x)))

df23['building_name']=df23['building_name'].apply(lambda x: x.split('वरील')[-1] if isinstance(x,str) else x)
df23['building_name']=df23['building_name'].apply(lambda x: x.split(',')[0])
reduce_noise(df23)

df24=df23[df23['building_name'].isnull()]

df23=df23[~df23['building_name'].isnull()]

df24['building_name']=df24['Property_description'].apply(lambda x: '|'.join(re.findall(r'\sअसलेल्या(.*?)इमारतीमधील',x)))

df24['building_name']=df24['building_name'].str.strip()
reduce_noise(df24)

df25=df24[df24['building_name'].isnull()]

df24=df24[~df24['building_name'].isnull()]

df25['building_name']=df25['Property_description'].apply(lambda x: '|'.join(re.findall(r'\sअसलेल्या(.*?)\sया\sनावाने\s',x)))

df25['building_name']=df25['building_name'].apply(lambda x: x.split('चौ. फूट त्यावर बांधलेली')[-1])
df25['building_name']=df25['building_name'].str.strip()
reduce_noise(df25)

df26=df25[df25['building_name'].isnull()]

df25=df25[~df25['building_name'].isnull()]

df26['building_name']=df26['Property_description'].apply(lambda x: '|'.join(re.findall(r'जागेवरील(.*?)मधील',x)))

df26['building_name']=df26['building_name'].apply(lambda x:x.split('केलेल्या ')[-1])
reduce_noise(df26)

df27=df26[df26['building_name'].isnull()]

df26=df26[~df26['building_name'].isnull()]

df27['building_name']=df27['Property_description'].apply(lambda x: '|'.join(re.findall(r'असलेल्या(.*?)इमारत',x)))

df27['building_name']=df27['building_name'].apply(lambda x:x.split('मधील')[0])
df27['building_name']=df27['building_name'].str.strip()
reduce_noise(df27)

df28=df27[df27['building_name'].isnull()]

df27=df27[~df27['building_name'].isnull()]

final_df1_df27=pd.concat([df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18,
                          df19,df20,df21,df22,df23,df24,df25,df26,df27],axis=0)

#final_df1_df27.to_sql('tbl_Maha_Final_Building_Name',engine2,if_exists='append',index=False)

df28['building_name']=df28['Property_description'].apply(lambda x: '|'.join(re.findall(r'जमिनीवरील(.*?)इमारत',x)))

df28['building_name']=df28['building_name'].apply(lambda x: x.split('वरील')[-1])
df28['building_name']=df28['building_name'].str.strip()
reduce_noise(df28)

df29=df28[df28['building_name'].isnull()]

df28=df28[~df28['building_name'].isnull()]

df29['building_name']=df29['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधलेल्या(.*?)इमारत',x)))

df29['building_name']=df29['building_name'].apply(lambda x: x.split('यांची मालकी वहिवाटीची')[0] if isinstance(x,str) else x)
reduce_noise(df29)


df30=df29[df29['building_name'].isnull()]

df29=df29[~df29['building_name'].isnull()]

df30['building_name']=df30['Property_description'].apply(lambda x: '|'.join(re.findall(r',[^,]+लि,',x)))

df30['building_name']=df30['building_name'].str.strip()
df30['building_name']=df30['building_name'].apply(lambda x: x.split('बांधलेले')[-1])
reduce_noise(df30)

df31=df30[df30['building_name'].isnull()]

df30=df30[~df30['building_name'].isnull()]

df31['building_name']=df31['Property_description'].apply(lambda x: '|'.join(re.findall(r'मधील(.*?मर्यादित)',x)))

df31['building_name']=df31['building_name'].apply(lambda x:x.split('झालेल्या')[-1])
df31['building_name']=df31['building_name'].str.strip()
reduce_noise(df31)

df32=df31[df31['building_name'].isnull()]

df31=df31[~df31['building_name'].isnull()]

df32['building_name']=df32['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधण्यात\sआलेल्या(.*?)इमारतीतील',x)))

df32['building_name']=df32['building_name'].apply(lambda x:x.split("म्हणजेच")[-1] if isinstance(x,str) else x)
df32['building_name']=df32['building_name'].apply(lambda x: None if isinstance(x,str) and 'आर सी सी' in x else x)
df32['building_name']=df32['building_name'].str.strip()
reduce_noise(df32)

df33=df32[df32['building_name'].isnull()]

df32=df32[~df32['building_name'].isnull()]

df33['building_name']=df33['Property_description'].apply(lambda x: '|'.join(re.findall(r'वरील(.*?)मधील',x)))

df33['building_name']=df33['building_name'].apply(lambda x: None if isinstance(x,str) and 'फ्लॅट' in x else x)
df33['building_name']=df33['building_name'].apply(lambda x:x.split('नावाने ओळखल्या जाणा-या')[0] if isinstance(x,str) else x)
df33['building_name']=df33['building_name'].str.strip()
reduce_noise(df33)

df34=df33[df33['building_name'].isnull()]

df33=df33[~df33['building_name'].isnull()]

df34['building_name']=df34['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधकाम(.*?या\sनावाने)',x)))

df34['building_name']=df34['building_name'].apply(lambda x: x.split('असेलेल्या')[-1] if isinstance(x,str) else x)
df34['building_name']=df34['building_name'].str.strip()
df34['building_name']=df34['building_name'].apply(lambda x:None if isinstance(x,str) and 'मोकळी जागा' in x else x)
reduce_noise(df34)

df35=df34[df34['building_name'].isnull()]

df34=df34[~df34['building_name'].isnull()]

df35['building_name']=df35['Property_description'].apply(lambda x: '|'.join(re.findall(r'येथील(.*?हौसिंग\sसोसायटी\sली\s)',x)))
df35['building_name']=df35['building_name'].apply(lambda x: x.split('निर्मित')[-1])
df35['building_name']=df35['building_name'].str.strip()
reduce_noise(df35)

df36=df35[df35['building_name'].isnull()]

df35=df35[~df35['building_name'].isnull()]

df36['building_name']=df36['Property_description'].apply(lambda x: '|'.join(re.findall(r',([^,]+\.\s?ली\.)',x)))

df36['building_name']=df36['building_name'].str.strip()
reduce_noise(df36)

df37=df36[df36['building_name'].isnull()]

df36=df36[~df36['building_name'].isnull()]

df37['building_name']=df37['Property_description'].apply(lambda x: '|'.join(re.findall(r',([^,]+\sसोसायटी\sली\.)',x)))

df37['building_name']=df37['building_name'].apply(lambda x: x.split('बांधलेला')[-1] if isinstance(x,str) else x)
df37['building_name']=df37['building_name'].str.strip()
df37['building_name']=df37['building_name'].apply(lambda x: None if isinstance(x,str) and 'लिहून' in x else x)
reduce_noise(df37)


df38=df37[df37['building_name'].isnull()]

df37=df37[~df37['building_name'].isnull()]

df38['building_name']=df38['Property_description'].apply(lambda x: '|'.join(re.findall(r'(.{30}\sरेसीडेन्सी)',x)))

df38['building_name']=df38['building_name'].str.strip()
reduce_noise(df38)

df39=df38[df38['building_name'].isnull()]

df38=df38[~df38['building_name'].isnull()]

df39['building_name']=df39['Property_description'].apply(lambda x: '|'.join(re.findall(r'येणा\-या(.*?)\sया\s',x)))

df39['building_name']=df39['building_name'].apply(lambda x:x.split('मिळकतींमध्ये')[-1] if isinstance(x,str) else x)
df39['building_name']=df39['building_name'].str.strip()
reduce_noise(df39)

df40=df39[df39['building_name'].isnull()]

df39=df39[~df39['building_name'].isnull()]

df40['building_name']=df40['Property_description'].apply(lambda x: '|'.join(re.findall(r'मजला(.*?)बिल्डिंग',x)))

df40['building_name']=df40['building_name'].str.strip()
reduce_noise(df40)

df41=df40[df40['building_name'].isnull()]

df40=df40[~df40['building_name'].isnull()]

df41['building_name']=df41['Property_description'].apply(lambda x: '|'.join(re.findall(r',[^,]+\sअपार्टमेंट',x)))

df41['building_name']=df41['building_name'].apply(lambda x:x.split('झालेला')[-1] if isinstance(x,str) else x)
df41['building_name']=df41['building_name'].str.strip()
df41['building_name']=df41['building_name'].apply(lambda x: None if isinstance(x,str) and 'अन्वये ' in x else x)
reduce_noise(df41)

df42=df41[df41['building_name'].isnull()]

df41=df41[~df41['building_name'].isnull()]

df42['building_name']=df42['Property_description'].apply(lambda x: '|'.join(re.findall(r'(.{40})\sया\sनावाने',x)))

df42['building_name']=df42['building_name'].apply(lambda x:x.split('नुसार')[-1])
df42['building_name']=df42['building_name'].apply(lambda x: None if 'असाइनमेंट' in x else x)
reduce_noise(df42)

df43=df42[df42['building_name'].isnull()]

df42=df42[~df42['building_name'].isnull()]

df43['building_name']=df43['Property_description'].apply(lambda x: '|'.join(re.findall(r'(.{40})\sया\sनावांने',x)))

df43['building_name']=df43['building_name'].apply(lambda x: x.split('यांनी')[-1])
df43['building_name']=df43['building_name'].apply(lambda x: x.split('मिळकतीवर')[-1])
df43['building_name']=df43['building_name'].str.strip('’')
reduce_noise(df43)

df44=df43[df43['building_name'].isnull()]

df43=df43[~df43['building_name'].isnull()]

df44['building_name']=df44['Property_description'].apply(lambda x: '|'.join(re.findall(r',([^,]+\sसोसायटी\sलीमीटेड?)',x)))

df44['building_name']=df44['building_name'].apply(lambda x: x.split('इतर माहिती: ')[-1])
df44['building_name']=df44['building_name'].str.replace('\n', ' ')
reduce_noise(df44)

df45=df44[df44['building_name'].isnull()]

df44=df44[~df44['building_name'].isnull()]

df45['building_name']=df45['Property_description'].apply(lambda x: '|'.join(re.findall(r',([^,]+\sसोसायटी)',x)))

df45['building_name']=df45['building_name'].apply(lambda x: x.split('बिल्टअप ')[-1] if isinstance(x,str) else x)
df45['building_name']=df45['building_name'].str.strip()
df45['building_name']=df45['building_name'].apply(lambda x: None if isinstance(x,str) and 'दिनांक' in x else x)
df45['building_name']=df45['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df45['building_name']=df45['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df45['building_name']=df45['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df45['building_name']=df45['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df45['building_name']=df45['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df46=df45[df45['building_name'].isnull()]

df45=df45[~df45['building_name'].isnull()]

df46['building_name']=df46['Property_description'].apply(lambda x: '|'.join(re.findall(r'वरील(.*?)बिल्डिंग',x)))

df46['building_name']=df46['building_name'].apply(lambda x: x.split('नामे वहिवाटीच्या')[0] if isinstance(x,str) else x)
df46['building_name']=df46['building_name'].str.strip()
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and 'पार्कींग' in x else x)
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df46['building_name']=df46['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df47=df46[df46['building_name'].isnull()]

df46=df46[~df46['building_name'].isnull()]

df47['building_name']=df47['Property_description'].apply(lambda x: '|'.join(re.findall(r'येथील(.*?)इमारतीमधील',x)))

df47['building_name']=df47['building_name'].str.strip()
df47['building_name']=df47['building_name'].apply(lambda x: None if isinstance(x,str) and 'नं.' in x else x)
df47['building_name']=df47['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df47['building_name']=df47['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df47['building_name']=df47['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df47['building_name']=df47['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df47['building_name']=df47['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df48=df47[df47['building_name'].isnull()]

df47=df47[~df47['building_name'].isnull()]

df48['building_name']=df48['Property_description'].apply(lambda x: '|'.join(re.findall(r'मिळकतीवरील(.*?)इमारतीतील',x)))

df48['building_name']=df48['building_name'].apply(lambda x: x.split(' या ')[0])
df48['building_name']=df48['building_name'].str.strip()
df48['building_name']=df48['building_name'].apply(lambda x: x.split('बांधण्यात आलेल्या')[-1] if isinstance(x,str) else x)
df48['building_name']=df48['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df48['building_name']=df48['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df48['building_name']=df48['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df48['building_name']=df48['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df48['building_name']=df48['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df49=df48[df48['building_name'].isnull()]

df48=df48[~df48['building_name'].isnull()]

df49['building_name']=df49['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधलेल्या(.*?)मधील',x)))

df2['building_name']=df2['building_name'].str.strip()
df49['building_name']=df49['building_name'].apply(lambda x: x.split('मजल्या')[-1] if isinstance(x,str) else x)
df49['building_name']=df49['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df49['building_name']=df49['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df49['building_name']=df49['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df49['building_name']=df49['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df49['building_name']=df49['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df50=df49[df49['building_name'].isnull()]

df49=df49[~df49['building_name'].isnull()]

df50['building_name']=df50['Property_description'].apply(lambda x: '|'.join(re.findall(r'असलेल्या(.*?)प्रकल्पातील',x)))

df50['building_name']=df50['building_name'].apply(lambda x: x.split(' या ')[0])
df50['building_name']=df50['building_name'].str.strip()
df50['building_name']=df50['building_name'].apply(lambda x: None if isinstance(x,str) and 'आर.सी.सी.' in x else x)
df50['building_name']=df50['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)>100 else x)
df50['building_name']=df50['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==0 else x)
df50['building_name']=df50['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==1 else x)
df50['building_name']=df50['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==2 else x)
df50['building_name']=df50['building_name'].apply(lambda x:None if isinstance(x,str) and len(x)==3 else x)

df51=df50[df50['building_name'].isnull()]

df50=df50[~df50['building_name'].isnull()]

df51['building_name']=df51['Property_description'].apply(lambda x: '|'.join(re.findall(r'करीत\sअसलेल्या\s(.*?)\sया\sस्कीम\sमधील',x)))

reduce_noise(df51)
df52=df51[df51['building_name'].isnull()]
df51=df51[~df51['building_name'].isnull()]

df52['building_name']=df52['Property_description'].apply(lambda x: '|'.join(re.findall(r'असलेल्या\s(.*?)\sया\sस्कीम\sमधील',x)))

df52['building_name']=df52['building_name'].apply(lambda x: x.split('|')[0])
df52['building_name']=df52['building_name'].str.replace("\'\'",'')
df52['building_name']=df52['building_name'].str.strip()
reduce_noise(df52)
df53=df52[df52['building_name'].isnull()]
df52=df52[~df52['building_name'].isnull()]

df53['building_name']=df53['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधण्यात\sयेणाऱ्या\s(.*?)\sया\sइमारतीतील',x)))

df53['building_name']=df53['building_name'].apply(lambda x: x.split('|')[0])
df53['building_name']=df53['building_name'].apply(lambda x: x.split(' केलेले ')[-1])
df53['building_name']=df53['building_name'].apply(lambda x: x.split(' या गृह')[0])
df53['building_name']=df53['building_name'].apply(lambda x: x.split(' या प्रक')[0])
reduce_noise(df53)
df54=df53[df53['building_name'].isnull()]
df53=df53[~df53['building_name'].isnull()]

df54['building_name']=df54['Property_description'].apply(lambda x: '|'.join(re.findall(r'येथील\s(.*?)\sया\sइमारतीतील|ईमरतीतील',x)))

df54['building_name']=df54['building_name'].apply(lambda x: x.split('|')[0])

df54['building_name']=df54['building_name'].apply(lambda x: x.split('आलेली ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('येथील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('येणारी ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('असणाऱ्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('केलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बाधलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('क्षेत्रातील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मिळकतीतील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('आलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('वरील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मधील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('या योजनेतील')[0])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मिळकतीवर ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('आलेले ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधलेली ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधसलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधकामातील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधलेल्य ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधालेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बाधंलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('झालेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधण्यातयेणाऱ्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('झालेली ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मध्ये बांधत')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मध्ये ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('यावर ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('म्हणजेच ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('संकुलातील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('पैकी ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('केेेलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('मिळकतीत ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('आणि ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('असलेली ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधलेले ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधकाम ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधीव ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('हद्दीतील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('असलेले ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('येणारे ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('यातील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('केलेली ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('इमारतीतील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('वहिवाटीची ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('परिसरातील ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('केलेले ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधण्यात ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('जाणा-या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('जाणाऱ्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('या नांवाने ओेळखल्या ')[0])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('अलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('बांधेलल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('येणाऱ्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('असलेल्या ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('येणारा ')[-1])
df54['building_name']=df54['building_name'].apply(lambda x: x.split(' या प्रकल्पातील')[0])
df54['building_name']=df54['building_name'].apply(lambda x: x.split('याप्रकल्पातील')[0])
df54['building_name']=df54['building_name'].apply(lambda x: x.split(' या गृहप्रकल्पातील')[0])

reduce_noise(df54)

df55=df54[df54['building_name'].isnull()]
df54=df54[~df54['building_name'].isnull()]

df55['building_name']=df55['Property_description'].apply(lambda x: '|'.join(re.findall(r'केलेल्या\s(.*?)या\sइमारती\sमधील',x)))

df55['building_name']=df55['building_name'].str.strip()
df55['building_name']=df55['building_name'].apply(lambda x: x.split('|')[0])

reduce_noise(df55)

df56=df55[df55['building_name'].isnull()]
df55=df55[~df55['building_name'].isnull()]

df56['building_name']=df56['Property_description'].apply(lambda x: '|'.join(re.findall(r'असलेल्या\s(.*?)\sया\sप्रकल्पाचे',x)))
df56['building_name']=df56['building_name'].apply(lambda x: x.split('|')[0])
reduce_noise(df56)

df57=df56[df56['building_name'].isnull()]
df56=df56[~df56['building_name'].isnull()]

df57['building_name']=df57['Property_description'].apply(lambda x: '|'.join(re.findall(r'आणि\s(.*?)\sया\sइमारतीतील',x)))
df57['building_name']=df57['building_name'].apply(lambda x: x.split('|')[0])
reduce_noise(df57)

df58=df57[df57['building_name'].isnull()]
df57=df57[~df57['building_name'].isnull()]

df58['building_name']=df58['Property_description'].apply(lambda x: '|'.join(re.findall(r'बांधण्यात\sयेणाऱ्या\s(.*?)\sया\sइमारतीच्या',x)))
df58['building_name']=df58['building_name'].apply(lambda x: x.split('|')[0])
reduce_noise(df58)

df59=df58[df58['building_name'].isnull()]
df58=df58[~df58['building_name'].isnull()]

df59['building_name']=df59['Property_description'].apply(lambda x: '|'.join(re.findall(r'मिळकती\sवरील\s(.*?)\sया\sप्रकल्पातील',x)))

df59['building_name']=df59['building_name'].apply(lambda x: x.split('|')[0])
df59['building_name']=df59['building_name'].apply(lambda x: x.split('मधिल ')[-1])
reduce_noise(df59)

df60=df59[df59['building_name'].isnull()]
df59=df59[~df59['building_name'].isnull()]

final_df28_df50=pd.concat([df28,df29,df30,df31,df32,df33,df34,df35,df36,df37,df38,df39,df40,df41,df42,df43,df44,
                           df45,df46,df47,df48,df49,df50],axis=0)

final=pd.concat([final_df1_df27,final_df28_df50],axis=0)

final['building_name']=final['building_name'].apply(lambda x: None if 'हॉर्स' in x else x)

final['building_name']=final['building_name'].str.replace('-',' ').str.replace('इतर माहिती','')

final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x,str) and 'ब्लॉक नं' in x else x)

final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x,str) and 'प्लॉट नं' in x else x)

final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x,str) and 'रोड नं' in x else x)

final['building_name']=final['building_name'].str.replace('"',' ').str.replace("'",' ').str.replace(',',' ').str.replace('-',' ')
final['building_name']=final['building_name'].str.replace('“',' ').str.replace('”',' ').str.replace(':',' ').str.replace(';',' ').str.replace('रोड नं','').str.replace('ब्लॉक नं',' ').str.replace('प्लॉट नं',' ')

final['building_name']=final['building_name'].str.replace('  ',' ').str.replace('‘‘','').str.replace('’’','').str.replace('‘’','')
final['building_name']=final['building_name'].str.replace('  ',' ')
final['building_name']=final['building_name'].str.strip()
final['building_name']=final['building_name'].str.strip('.')
final['building_name']=final['building_name'].str.strip('/')
final['building_name']=final['building_name'].str.strip()

final['building_name']=final['building_name'].apply(lambda x: None if x=='प्रकल्पा' else x)
final['building_name']=final['building_name'].apply(lambda x: None if x=='या प्रोजेक्ट'  else x)

final['building_name']=final['building_name'].apply(lambda x: x.split('|')[0] if isinstance(x,str) else x)

final['building_name']=final['building_name'].apply(lambda x: None if x=='इतर माहिती' else x)
final['building_name']=final['building_name'].apply(lambda x: None if x=='रोड' else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and'गट नंबर'  in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'असाईनमेंट' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'अग्रीमेंट'  in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'आर सी सी' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'चौ मी' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'दस्ता' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'आर.सी.सी' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'करारनामा' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'पाचव्या ' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'दहाव्या ' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'सातव्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'पहिल्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'आठव्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'एकोणाविसाव्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'एकोणिसाव्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'बाविसाव्या' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'सिमेंट' in x else x)
final['building_name']=final['building_name'].apply(lambda x: None if isinstance(x, str) and 'मंजूर लेआऊट' in x else x)
reduce_noise(final)

final=final[~final['building_name'].isnull()]

final_df28_df50.to_sql('tbl_Maha_Final_Building_Name',engine2,if_exists='append',index=False)