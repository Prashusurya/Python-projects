# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 11:09:08 2024

@author: spras
"""

import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

service = Service()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
driver.get("https://www.trivago.in/en-IN/srl/hotels-mumbai-india?search=200-64981;dr-20240430-20240501")
driver.maximize_window()
time.sleep(10)

close_button=driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/main/div[1]/div[2]/div/div[3]/div/div[2]/div/div/button/span[2]').click()
time.sleep(1)

check_in_out=driver.find_elements(By.XPATH,'//button[@class="group w-full text-left truncate h-15 px-0 bg-white active:bg-grey-200"]')
check_in_out[0].click()
time.sleep(2)
check_in_out_date=driver.find_elements(By.XPATH,'//label[@class="max-w-full text-s font-bold px-4 py-1 cursor-pointer disabled:cursor-not-allowed leading-none inline-flex items-center"]')
#select any date from the 4 options for checkin
check_in_out_date[0].click()
time.sleep(3)
#now select check out date
check_in_out[1].click()
time.sleep(1)
checkout_date=driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/main/div[1]/div[2]/div/div[3]/div/div[2]/div/div/div/div[3]/ul/li[2]')
checkout_date.click()
time.sleep(2)
search=driver.find_element(By.XPATH,'//button[@class="SearchButton_button__ldRRI SearchButton_buttonWithoutIcon__VdR_v"]')
search.click()
time.sleep(10)

all_hotel_types=[]
all_hotel_names=[]
all_prices=[]
all_ratings=[]

def extract():
    hotel_type=driver.find_elements(By.XPATH,'//*[@class="info-section_infoSection__sY1hr info-section_margin__rNbKo"]')
    time.sleep(1)
    hotel_name=driver.find_elements(By.XPATH,'//*[@class="item-name_button__1e5cV truncate_truncate__vCzPM"]//*')
    time.sleep(1)
    hotel_price=driver.find_elements(By.XPATH,'//*[@class="Price_price__gzSVe Price_large__cM2EH Price_bold__cJ2IU"]')
    time.sleep(1)
    hotel_rating=driver.find_elements(By.XPATH,'//strong[@class="leading-none"]//*')
    time.sleep(1)
    for h_type in hotel_type:
        all_hotel_types.append(h_type.text)
    for h_name in hotel_name:
        all_hotel_names.append(h_name.text)
    for h_price in hotel_price:
        all_prices.append(h_price.text)
    for  h_rating in hotel_rating:
        all_ratings.append(h_rating.text)
    time.sleep(2)

try:
    extract()
    next_page=driver.find_element(By.XPATH,'//*[@id="__next"]/div[1]/main/div[3]/section/div/div/div/nav/ol/li[6]/button')
    next_page.click()
    time.sleep(10)
except:
    pass

driver.close()

df=pd.DataFrame({"hotel name":all_hotel_names, "hotel type":all_hotel_types, "hotel price":all_prices, "hotel rating":all_ratings})


