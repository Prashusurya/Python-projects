import re
import csv
import time
from selenium import webdriver
from datetime import date
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
quarter='Jan-March'
path = 'D:/99A script/'
#"Delhi","Gurgaon","Noida","Greater Noida","Ghaziabad","Faridabad","Mumbai","Bangalore","Chennai","Hyderabad",
#"Pune","Kolkata","Ahmedabad","Bhubaneswar","Coimbatore","Indore","Nagpur","Vadodara","Chandigarh","Jaipur","Lucknow","Surat"
city_dict = {#'Nasik':'https://www.99acres.com/property-rates-and-price-trends-in-nasik-prffid',
             # 'Delhi':'https://www.99acres.com/property-rates-and-price-trends-in-delhi-prffid',
             # 'Gurgaon':'https://www.99acres.com/property-rates-and-price-trends-in-gurgaon-prffid',
             # 'Noida':'https://www.99acres.com/property-rates-and-price-trends-in-noida-prffid',
             # 'Greater Noida':'https://www.99acres.com/property-rates-and-price-trends-in-greater-noida-prffid',
             # 'Ghaziabad':'https://www.99acres.com/property-rates-and-price-trends-in-ghaziabad-prffid',
             # 'Faridabad':'https://www.99acres.com/property-rates-and-price-trends-in-faridabad-prffid',
             # 'Bangalore':'https://www.99acres.com/property-rates-and-price-trends-in-bangalore-prffid',
             #'Chennai':'https://www.99acres.com/property-rates-and-price-trends-in-chennai-prffid',
             #'Hyderabad':'https://www.99acres.com/property-rates-and-price-trends-in-hyderabad-prffid',
             #'Pune':'https://www.99acres.com/property-rates-and-price-trends-in-pune-prffid',
             #'Kolkata':'https://www.99acres.com/property-rates-and-price-trends-in-kolkata-prffid',
             #'Ahmedabad':'https://www.99acres.com/property-rates-and-price-trends-in-ahmedabad-prffid',
             #'Bhubaneshwar':'https://www.99acres.com/property-rates-and-price-trends-in-bhubaneswar-prffid',
             #'Coimbatore':'https://www.99acres.com/property-rates-and-price-trends-in-coimbatore-prffid',
             #'Indore':'https://www.99acres.com/property-rates-and-price-trends-in-indore-prffid',
             #'Nagpur':'https://www.99acres.com/property-rates-and-price-trends-in-nagpur-prffid',
             #'Vadodara':'https://www.99acres.com/property-rates-and-price-trends-in-vadodara-prffid',
             #'Chandigardh':'https://www.99acres.com/property-rates-and-price-trends-in-chandigarh-prffid',
             #'Jaipur':'https://www.99acres.com/property-rates-and-price-trends-in-jaipur-prffid',
             #'Lucknow':'https://www.99acres.com/property-rates-and-price-trends-in-lucknow-prffid',
             #'Surat':'https://www.99acres.com/property-rates-and-price-trends-in-surat-prffid',
             'Mumbai':'https://www.99acres.com/property-rates-and-price-trends-in-mumbai-prffid',
        }

#print(citynm, 'corresponds to', link)
date_today=date.today().strftime("%d/%m/%Y")
print('======================================================================')
print('Running 99 Acres Script...')
print('======================================================================')
print('Started:',str(date_today))
print('----------------------------------------------------------------------')
for citynm, link in city_dict.items():
    print('Current City Name:', citynm)
    driver = webdriver.Chrome()
    #driver = webdriver.Firefox()
    driver.maximize_window()
    driver.get(str(link))
    time.sleep(3)
    original_window = driver.current_window_handle
    #Resultfile = open(filenamedate+"_Residential.csv",'a',encoding='utf-8',newline='') 
    #head = csv.writer(Resultfile)
    #head.writerow(["City","Locality","Type","Avg Price","Units","Source"])
    txt = driver.find_element(By.XPATH,'//*[@id="RLP_DEFAULT_WRAPPER"]/div[6]/div[2]/div/div[1]/div[1]')
    #print('Text Found:',txt.text)
    txt = txt.text.split(' ')
    itm = int(txt[2])
    print('Locations Found:',itm)
    SCROLL_PAUSE_TIME = 5 # You can set your own pause time. My laptop is a bit slow so I use 1 sec
    screen_height = driver.execute_script("return window.screen.height;")   # get the screen height of the web
    # print("Screen Height:",screen_height)
    k = 1             
    while True:
        k+=1
        time.sleep(SCROLL_PAUSE_TIME)
        try:
            driver.execute_script("return arguments[0].scrollIntoView(true);", WebDriverWait(driver,20).until(EC.visibility_of_element_located((By.XPATH, '//*[@id="RLP_DEFAULT_WRAPPER"]/div[6]/div[2]/div/button'))))
            driver.execute_script("arguments[0].click();", WebDriverWait(driver,20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="RLP_DEFAULT_WRAPPER"]/div[6]/div[2]/div/button'))))
            # print("Show More button clicked")
            time.sleep(2)
        except Exception:
            pass
                                
        scroll_height = driver.execute_script("return document.body.scrollHeight;")
        # print("Scroll Height:", scroll_height)
        time.sleep(3)
        currloc = driver.find_elements(By.XPATH,'//*[@class="rT__ptTuple"]')
        driver.execute_script("window.scrollTo(0, {screen_height}*{k});".format(screen_height=screen_height, k=k))
        #print('Length:',len(currloc))
        time.sleep(3)
        if(len(currloc)==itm):
            break
        # Break the loop when the height we need to scroll to is larger than the total scroll height
        elif (screen_height) * k > scroll_height:
            break
        #driver.execute_script("window.scrollTo(0, {screen_height}*{k});".format(screen_height=screen_height, k=k))  
        time.sleep(3)
    print('Done Scroll')
    print('Locations Found :',len(currloc))
    Resultfile = open(path+"_"+citynm+"_"+quarter+"_Apartment.csv",'a',encoding='utf-8',newline='') 
    head = csv.writer(Resultfile)
    head.writerow(["Date","Quarter Period","Type","City","Zone","Locality","Avg Price","Unit","Source","class_category"])
    loclist=[]
    time.sleep(1)
    #dropdwnbtn = driver.find_elements(By.XPATH,'//*[@class="pageComponent reiTuples__circularArrw "]')
    time.sleep(1)
    zone_name = driver.find_elements(By.XPATH,'//*[@class="rT__sl"]')
    time.sleep(1)
    locality_name= driver.find_elements(By.XPATH,'//*[@class="section_header_semiBold spacer2"]')
    time.sleep(1)
    quarter_pr = driver.find_elements(By.XPATH,'//*[@class="rT__w2"]/div[2]')
    time.sleep(1)
    class_category=driver.find_elements(By.XPATH,'//*[@class="rT__bw"]')
    time.sleep(1)
    #YoY = driver.find_elements_by_xpath('//*[@class="priceTrendsSmallGraph__chartTxt  list_header_semiBold"]')
    time.sleep(1)
    val=len(quarter_pr)
    # print('Val:',val)
    wr = csv.writer(Resultfile)
    for j in range(0,val):
        #print('J:',j)
        loclist.append(date_today)
        loclist.append(quarter)
        loclist.append("Apartment")
        loclist.append(citynm)
        loclist.append(zone_name[j].text)
        loclist.append(locality_name[j].text)
        # print(quarter_pr[j].text)
        #print('ELELELE:',quarter_pr[j].text)
        if(quarter_pr[j].text.split(' ')[0].strip()=='NA'):
            loclist.append('Not Available')
        else:
            price = quarter_pr[j].text.split(' ')[0]
            price =  ",". join(re.findall(r'\d+',price))
            loclist.append(price)#.replace('Avg. Rate','')
        if(quarter_pr[j].text.split(' ')[0].strip()=='NA'):
            loclist.append('Not Available')
        else:
            ut = quarter_pr[j].text.split(' ')[1]
            #print("UTT:",ut)
            ut =  ut.split('/')[0]
            loclist.append(ut)
        loclist.append("https://www.99acres.com")
        time.sleep(1)
        try:
            loclist.append(class_category[j].text)
        except:
            loclist.append("Not Available")
        time.sleep(1)
        wr.writerow(loclist)
        loclist=[]
    Resultfile.close()
    driver.close()
    time.sleep(3)