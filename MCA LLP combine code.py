# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 01:16:47 2024

@author: Administrator
"""
import glob
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote

engine = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))

query="SELECT * FROM  altrr_database.tbl_mca_llp_master_data"

original=pd.read_sql(query, con=engine)
original=original.drop_duplicates(subset=list(original.columns))

all_files = glob.glob(r'C:\Prashant\MCA_LLP' + "/*.xlsx")
master_data = pd.DataFrame()
IndexOfCharges = pd.DataFrame()
Director_details = pd.DataFrame()
irp = pd.DataFrame()

for file in all_files:
    df = pd.read_excel(file, sheet_name='MasterData')
    temp_df=df.copy()
    temp_df=temp_df.drop('Unnamed: 2', axis=1)
    temp_df = temp_df[0:12].T.reset_index(drop=True)
    cols = temp_df.iloc[0].to_list()
    temp_df = temp_df[1:]
    temp_df.columns = cols
    temp1=df[12:15]
    temp1=temp1.drop('Unnamed: 2', axis=1)
    cols = temp1.iloc[1].to_list()
    temp1.columns = cols
    temp1 = temp1[2:]
    temp1=temp1.rename(columns={'Date of filing':'accounts_and_solvency_filing_date','Financial Year':'accounts_and_solvency_filing_year'})
    temp1=temp1.reset_index()
    temp1=temp1.drop('index',axis=1)
    temp_df=temp_df.reset_index()
    temp_df=temp_df.drop('index',axis=1)
    
    temp2=df[15:18]
    temp2=temp2.drop('Unnamed: 2', axis=1)
    cols = temp2.iloc[1].to_list()
    temp2.columns = cols
    temp2 = temp2[2:]     
    temp2=temp2.rename(columns={'Date of filing':'annual_returns_filing_date','Financial Year':'annual_returns_filing_year'})
    temp2=temp2.reset_index()
    temp2=temp2.drop('index',axis=1)
    temp_df = temp_df.join(temp1, lsuffix='temp_df', rsuffix='temp1')
    temp_df = temp_df.join(temp2, lsuffix='temp_df', rsuffix='temp2')
    
    
    temp3=df[19:21]
    temp3=temp3.drop('Unnamed: 2', axis=1)
    temp3 = temp3.T.reset_index(drop=True)
    cols = temp3.iloc[0].to_list()
    temp3 = temp3[1:]
    temp3.columns = cols
    temp3=temp3.reset_index()
    temp3=temp3.drop('index',axis=1)
    temp_df = temp_df.join(temp3, lsuffix='temp_df', rsuffix='temp3')
    
    llp_number = ''.join(temp_df['LLPIN'][0])
    master_data = pd.concat([master_data, temp_df], axis=0, ignore_index=True)
    
    
    dftwo = df[23:].reset_index(drop=True)
    cols1 = dftwo.iloc[0].to_list()
    dftwo = dftwo[1:]
    dftwo.columns = cols1
    irp = pd.concat([irp, dftwo], axis=0, ignore_index=True)
    df1 = pd.read_excel(file, sheet_name='IndexOfCharges').reset_index(drop=True)
    cols = df1.iloc[0].to_list()
    df1 = df1[1:]
    df1.columns = cols
    df1['LLPIN'] = llp_number
    IndexOfCharges = pd.concat([IndexOfCharges, df1], axis=0, ignore_index=True)
    df2 = pd.read_excel(file, sheet_name='Director Details').reset_index(drop=True)
    cols = df2.iloc[0].to_list()
    df2 = df2[1:]
    df2.columns = cols
    df2['LLPIN'] = llp_number
    Director_details = pd.concat([Director_details, df2], axis=0, ignore_index=True)    


master_data.to_excel(r'C:\Prashant\llp_master_data_combined_new.xlsx', index=False)
IndexOfCharges.to_excel(r'C:\Prashant\llp_index_of_charges_combined_new.xlsx', index=False)
Director_details.to_excel(r'C:\Prashant\llp_director_details_combined_new.xlsx', index=False)
#irp.to_excel(r'C:\Prashant\llp_irp_combined.xlsx', index=False)

Director_details=Director_details.rename(columns={'Sr. No':'Sr_No', 'DIN/PAN':'DIN_PAN', 'Date of Appointment':'Date_of_Appointment',
                                                  'Cessation Date':'Cessation_Date', 'LLPIN':'CIN'})

Director_details.to_sql("tbl_mca_director_details", con=engine, if_exists='append', index=False)

IndexOfCharges=IndexOfCharges.rename(columns={'Sr. No':'Sr_No', 'LLPIN':'CIN', 'Charge Identification number':'Charge_Id',
                                              'Charge Holder Name':'Charge_Holder_Name', 'Date of Creation':'Date_of_Creation',
                                              'Date of Modification':'Date_of_Modification', 'Date of Satisfaction':'Date_of_Satisfaction',
                                              'Whether charge registered by other entity':'Whether_charge_registered_by_other_entity',
                                              'Asset Holder Name':'Asset_Holder_Name'})

IndexOfCharges.to_sql("tbl_mca_index_of_charges", con=engine, if_exists='append', index=False)

master_data=master_data.rename(columns={'LLPIN':'CIN', 'LLP Name':'Company_Name', 'ROC Name':'ROC_Name', 'Date of Incorporation':'Date_of_Incorporation',
                                        'Email Id':'Email_Id', 'Registered Address':'Registered_Address', 'LLP Status':'Company_Status',
                                        'ROC (name and office)':'ROC_name_and_office', 'RD (name and Region)':'RD _name_and_Region'})

master_data=master_data.rename(columns={'Number of Partners':'Number_of_Partners', 'Number of Designated Partners':'Number_of_Designated_Partners',
                                        'Total Obligation of Contribution':'Total_Obligation_of_Contribution', 'Strike off/amalgamated/transferred date':'Strike_off_date',
                                        'Status under CIRP':'Status_under_Cirp'})

master_data.to_sql("tbl_mca_master_data", con=engine, if_exists='append', index=False)

from sqlalchemy import create_engine
from urllib.parse import quote

engine = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))
query="SELECT * FROM  altrr_database.tbl_mca_director_details"
df=pd.read_sql(query, engine)
df.columns
df.drop('CrDt', axis=1, inplace=True)

df1=pd.read_excel(r'C:\Prashant\llp_director_details_combined.xlsx')
df1.columns=list(df.columns)

df1=df1.rename(columns={'Accounts_and_Solvency_Filing_InformationDate_of_filingFinancial_Year':'Accounts_and_Solvency_Filing'})
df1.to_sql('tbl_mca_llp_director_details',con=engine,index=False,if_exists='append')
