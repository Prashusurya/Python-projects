# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 13:09:43 2024

@author: pc
"""
import requests 
from bs4 import BeautifulSoup
import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from PIL import Image
import pytesseract
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
import base64
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import undetected_chromedriver as uc
from sqlalchemy import create_engine
from urllib.parse import quote
from fake_useragent import UserAgent
import warnings
warnings.filterwarnings("ignore")

engine = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

service = Service()
options = webdriver.ChromeOptions()
ua = UserAgent()
user_agent = ua.random
print(user_agent)
options.add_argument(f'user-agent={user_agent}')
#options.add_argument('--headless')
options.add_argument("--window-size=1920x1080")
driver = webdriver.Chrome(service=service, options=options)
driver.delete_all_cookies()
time.sleep(3)
driver.get("https://wbregistration.gov.in/(S(2xe2amrl1bhepma1trv3htlh))/index/SearchBy_QueryNo_DeedNo.aspx")
#driver.maximize_window()
time.sleep(3)

deed_type=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_RadioButtonList1_1']")
deed_type.click()
time.sleep(5)

sel_dist=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_DDL_Dist"]')
sel_dist.send_keys('Birbhum                       ')
time.sleep(3)

sel_reg_office=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_DDL_RO"]')
sel_reg_office.send_keys('NALHATI (A.D.S.R.)')
time.sleep(2)

deed_year=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_txt_deed_year"]')
deed_year.send_keys('2024')
time.sleep(1)


# final_df1=pd.DataFrame()
# final_df2=pd.DataFrame()
# final_df3=pd.DataFrame()


for i in range(1,15000):
    try:
        input_deedno=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_txt_deedno"]')
        input_deedno.click()
        time.sleep(1)
        input_deedno.send_keys(Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE)
        time.sleep(0.5)
        input_deedno=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_txt_deedno"]')
        input_deedno.send_keys(i)
        print('current document number:',i)
        time.sleep(1)
        
        tries=0
        while tries<4:
            try:
                driver.execute_script("window.scrollTo(0, 200)")
                time.sleep(1)
                with open('captcha1.png', 'wb') as file:
                    file.write(driver.find_element(By.XPATH,'/html/body/form/div[3]/div[3]/div/div[2]/div/center/div/div/div[3]/div/div[3]/img').screenshot_as_png)
                im=Image.open('captcha1.png')
                text = pytesseract.image_to_string(im, lang = 'eng')
                text=text.rstrip()
                print(text)
                
                captcha_box=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_txtCapcha']")
                captcha_box.clear()
                time.sleep(0.5)
                captcha_box.send_keys(text)
                time.sleep(0.5)
                
                submit_button=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_btnSubmitQuery']")
                submit_button.click()
                time.sleep(3)
                #driver.execute_script("window.scrollTo(0, 200)")
                
                try:
                    not_available2=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_lbl_msg_Prop']")                        
                    if not_available2.is_displayed():
                        print('No Record Found for Property')
                        break
                except:
                    pass
                
                try:
                    not_available1=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_lbl_msg']").text.lower()
                    if 'no record found for name' in not_available1:
                        print('No Record Found for Name')
                        break
                except:
                    pass
                
                document_Details=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_lbl_deeddetails']")
                if document_Details.is_displayed():
                    print('Document details are visible, extracting data...')
                    driver.execute_script("window.scrollTo(0, 200)")
                    time.sleep(1)
                    table1=driver.find_element(By.XPATH,"//table[@id='ctl00_CPH_grv_deeddetails']").get_attribute('outerHTML')
                    df=pd.read_html(table1)
                    df1=df[0]
                    df1=df1.drop('Volume & Page No', axis=1)
                    df1=df1.rename(columns={'Transaction':'Transaction_Type', 'Registered At':'SRO_Name',
                                  'Deed No':'Document_No', 'Date of Registration':'Registration_Date',
                                  'Date of Completion':'Completion_Date', 'Date of Delivery':'Delivery_Date',
                                  'Serial No':'Serial_No'})
                    deed_no=''.join(df1['Document_No'])
                    #print(df1)
                    df1.to_sql('tbl_west_bengal_igr_basic_details',con=engine,index=False,if_exists='append')

                    #final_df1=pd.concat([final_df1,df1],axis=0, ignore_index=True)
                    #################################################################################################################
                    #print(df)
                    table2=driver.find_element(By.XPATH,"//table[@id='ctl00_CPH_GRV_SearchByName']").get_attribute('outerHTML')
                    df4=pd.read_html(table2)
                    df5=df4[0]
                    df5['Document_No']=deed_no 
                    #print(df5)
                    df5.to_sql('tbl_west_bengal_igr_party_details',con=engine,index=False,if_exists='append')
                    #final_df3=pd.concat([final_df3,df5], axis=0, ignore_index=True)
                    
                    #################################################################################################################
                    table3=driver.find_element(By.XPATH,"//table[@id='ctl00_CPH_GRV_SearchByProperty']").get_attribute('outerHTML')
                    df2=pd.read_html(table3)
                    df3=df2[0]
                    df3['Document_No']=deed_no
                    df3=df3.rename(columns={'Property Location':'Property_Location', 'Property Type & Transaction':'Property_Type_And_Transaction',
                                            'Plot & Khatian No and Zone':'Plot_And_Khatian_No', 'Area of Property':'Property_Area'})
                    #print(df3)
                    df3.to_sql('tbl_west_bengal_igr_property_details',con=engine,index=False,if_exists='append')
                    #final_df2=pd.concat([final_df2,df3],axis=0, ignore_index=True)
                    captcha_refresh=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_btnImage']")
                    captcha_refresh.click()
                    time.sleep(3)
                    deed_type=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_RadioButtonList1_1']")
                    deed_type.click()
                    time.sleep(1)
                    #input_deedno.send_keys(Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE+Keys.BACKSPACE)
                    #print(df[0]['Deed No'])
                    pass
                break
                                                           
            except:
                print('captcha failed, retrying...')
                tries+=1
    except:
        driver.quit()
        time.sleep(5)
        driver.get("https://wbregistration.gov.in/(S(2xe2amrl1bhepma1trv3htlh))/index/SearchBy_QueryNo_DeedNo.aspx")
        time.sleep(5)
        deed_type=driver.find_element(By.XPATH,"//*[@id='ctl00_CPH_RadioButtonList1_1']")
        deed_type.click()
        time.sleep(5)

        sel_dist=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_DDL_Dist"]')
        sel_dist.send_keys('Birbhum                       ')
        time.sleep(5)

        sel_reg_office=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_DDL_RO"]')
        sel_reg_office.send_keys('NALHATI (A.D.S.R.)')
        time.sleep(5)

        deed_year=driver.find_element(By.XPATH,'//*[@id="ctl00_CPH_txt_deed_year"]')
        deed_year.send_keys('2024')
        time.sleep(1)
