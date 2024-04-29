
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
from selenium.webdriver.common.keys import Keys

distcols = ["District", "District 1", "District 2", "District 3", "District 4", "District 5", "District 6", "District 7", "District 8", "District 9", "District 10", "District 11", "District 12", "District 13", "District 14", "District 15", "District 16", "District 17", "District 18", "District 19", "District 20", "District 21", "District 22"]

df=pd.read_excel(r"C:\Users\Admin\Desktop\IMD Pune\Data\all district list(imd pune).xlsx", sheet_name='Webiste Obtained')

for distrct in distcols:
    service = Service()
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)
    driver.get("https://imdpune.gov.in/hazardatlas/lightvul/annual/days/index.html")
    #https://imdpune.gov.in/hazardatlas/flood/events/index.html
    #https://imdpune.gov.in/hazardatlas/cold_wave/annual/days/index.html
    #https://imdpune.gov.in/hazardatlas/heat_wave/annual/days/index.html
    driver.maximize_window()
    time.sleep(5)

    actions=ActionChains(driver)
    state=[]
    districts=[]
    dist_id=[]
    heatwaves=[]
    for i, row in df.iterrows():
        dist=row[distrct]
        try:
            main_map=driver.find_element(By.XPATH,'//*[@id="map"]')
            search_button=driver.find_element(By.XPATH,'//*[@id="map"]/div/div[2]/div[7]/button')
            time.sleep(1)
            search_button.click()
            time.sleep(1)
            input_box=driver.find_element(By.XPATH,'//*[@id="ol-search-input"]')
            time.sleep(1)
            #input_box.send_keys(dist)
            district = list(dist)#dist.split() #list(dist)
            for lett in district:    
                input_box.send_keys(lett)
                time.sleep(1)
            time.sleep(1)
            #currtxt = driver.find_elements(By.XPATH,'//*[@class="sey-item sey-selected"]')
            time.sleep(1)
            # if(len(currtxt)==0):
            #     continue
            time.sleep(2)
            while(True):
                current_txt = driver.find_elements(By.XPATH,'//*[@class="sey-item sey-selected"]/span')
                current_txt = "".join([i.text for i in current_txt])
                print("Current Text",current_txt)
                if(dist.replace(' ','').lower()==current_txt.lower()):
                    input_box.send_keys(Keys.ENTER)
                    time.sleep(1)
                    break
                else:
                    input_box.send_keys(Keys.ARROW_DOWN)
                    time.sleep(1)
            time.sleep(3)
            actions.move_to_element_with_offset(main_map, 0, 0).perform()
            time.sleep(3)
            state_name=driver.find_element(By.XPATH,'//*[@id="popup-content"]/table/tbody/tr[1]/td')
            state.append(state_name.text)
            time.sleep(1)
            dist_name=driver.find_element(By.XPATH,'//*[@id="popup-content"]/table/tbody/tr[2]/td')
            districts.append(dist_name.text)
            time.sleep(1)
            district_id=driver.find_element(By.XPATH,'//*[@id="popup-content"]/table/tbody/tr[3]/td')
            dist_id.append(district_id.text)
            time.sleep(1)
            heatwave=driver.find_element(By.XPATH,'//*[@id="popup-content"]/table/tbody/tr[4]/td')
            heatwaves.append(heatwave.text)
            time.sleep(1)
            input_box.clear()
            time.sleep(1)            
        except Exception as e:
            print(e)
            
    df1=pd.DataFrame({"State":state, "District":districts, "district ID":dist_id, "lightning":heatwaves})   
    df1.drop_duplicates(inplace=True)
    df1.to_excel(r'C:\Users\Admin\Desktop\IMD Pune\Data\India lightining '+distrct+'.xlsx')
    driver.close()
    time.sleep(300)
