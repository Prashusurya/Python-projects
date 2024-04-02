
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd

path=r'E:\NDVI vedas modis script\data\\'

service = Service()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
driver.get("https://vedas.sac.gov.in/vegetation-monitoring/index.html")
driver.maximize_window()
time.sleep(5)

reference_layers=driver.find_element(By.XPATH,'//*[@id="tab-controls"]/div[1]/button/span')
reference_layers.click()
time.sleep(1)

dist_bound=driver.find_element(By.XPATH,'//*[@id="app"]/div[6]/div[2]/div[4]/label/span')
dist_bound.click()
time.sleep(1)

data_visualize=driver.find_element(By.XPATH,'//*[@id="tab-controls"]/div[2]/button')
data_visualize.click()
time.sleep(1)

# theme=Select(driver.find_element(By.XPATH,'//*[@id="app"]/div[6]/div[2]/div[1]/select'))
# time.sleep(3)
# theme.select_by_visible_text('Soil Moisture')
# # awifs_15.select_by_index(1)    
# time.sleep(5)

modis_16=Select(driver.find_element(By.XPATH,'//*[@id="app"]/div[6]/div[2]/div[2]/select'))
modis_16.select_by_visible_text('MODIS - 16 Day Max NDVI [250m]')
time.sleep(3)

dist_level=Select(driver.find_element(By.XPATH,'//*[@id="app"]/div[6]/div[2]/div[6]/div/select'))
time.sleep(1)
dist_level.select_by_visible_text('District Level')
time.sleep(2)

#read the excel file "district wise lat long here"
df=pd.read_excel(r"E:\NDVI vedas modis script\district-wise-lat-long.xlsx", sheet_name='Uttar Pradesh')
time.sleep(1)

elem = driver.find_element(By.XPATH,'//*[@id="map"]')

#create empty lists to append data
State=[]
District=[]
current_year=[]
Dates=[]
NDVI_value=[]

#function to loop through year to year charts
def year_loop(year):
    chart=driver.find_element(By.XPATH,'//*[local-name()="svg"]//*[name()="g" and @class="highcharts-series-group"]')
    gettoplefty=int(chart.size['height']/2)-(chart.size['height'])
    gettopleftx=int(chart.size['width']/2)-(chart.size['width'])
    # print(gettoplefty, gettopleftx)
    # print(chart.location, chart.size)
    actions.move_to_element_with_offset(chart, gettopleftx+25, gettoplefty).perform()
    current_year.append(year)
    State.append(State_name)
    District.append(District_name)
    try:
        current_date=driver.find_element(By.XPATH,'//*[@style="font-size:12px;color:#333333;fill:#333333;"]//*[1]').text
        Dates.append(str(current_date))
        values=driver.find_element(By.XPATH,'//*[@style="font-size:12px;color:#333333;fill:#333333;"]//*[2]').text
        NDVI_value.append(str(values))
        time.sleep(0.5)
    except:
        NDVI_value.append('not available')
        Dates.append('not available')
    for i in range(0,1500,50):
        current_year.append(year)
        actions.move_to_element_with_offset(chart, gettopleftx+i, gettoplefty).perform()            
        State.append(State_name)
        District.append(District_name)            
        try:
            current_date=driver.find_element(By.XPATH,'//*[@style="font-size:12px;color:#333333;fill:#333333;"]//*[1]').text
            Dates.append(str(current_date))
            values=driver.find_element(By.XPATH,'//*[@style="font-size:12px;color:#333333;fill:#333333;"]//*[2]').text
            NDVI_value.append(str(values))
            time.sleep(0.5)
        except:
            NDVI_value.append('not available')
            Dates.append('not available')

for i, row in df.iterrows():
    State_name=row['State']
    District_name=row['District']
    Long=row['Long']
    Lat=row['Lat']
    location=driver.find_element(By.XPATH,'//*[@id="search-box"]')
    location.send_keys(Long,',', Lat)
    time.sleep(1)
    search_go=driver.find_element(By.XPATH,'//*[@id="main-content"]/div[8]/button')
    search_go.click()
    time.sleep(5)
    actions = ActionChains(driver)
    actions.move_to_element_with_offset(elem, 10, 0).click().perform()
    time.sleep(10)
    year_2020=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-0 highcharts-series-20"]')
    year_2020.click()
    time.sleep(0.5)
    year_2021=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-1 highcharts-series-21"]')
    year_2021.click()
    time.sleep(0.5)
    year_2022=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-2 highcharts-series-22"]')
    year_2022.click()
    time.sleep(0.5)
    # year_loop(year='2023')
    year_2023=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-3 highcharts-series-23"]')
    year_2023.click()
    time.sleep(0.5)
    year_2015bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-5 highcharts-series-15 highcharts-legend-item-hidden"]')
    year_2015bc.click()
    time.sleep(1)
    year_loop('2015')
    year_2015ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-5 highcharts-series-15 "]')
    year_2015ac.click()
    time.sleep(0.5)
    year_2016bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-6 highcharts-series-16 highcharts-legend-item-hidden"]')
    year_2016bc.click()
    time.sleep(1)
    year_loop('2016')
    year_2016ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-6 highcharts-series-16 "]')
    year_2016ac.click()
    time.sleep(0.5)
    year_2017bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-7 highcharts-series-17 highcharts-legend-item-hidden"]')
    year_2017bc.click()
    time.sleep(1)
    year_loop('2017')
    year_2017ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-7 highcharts-series-17 "]')
    year_2017ac.click()
    time.sleep(0.5)
    year_2018bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-8 highcharts-series-18 highcharts-legend-item-hidden"]')
    year_2018bc.click()
    time.sleep(1)
    year_loop('2018')
    year_2018ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-8 highcharts-series-18 "]')
    year_2018ac.click()
    time.sleep(0.5)
    year_2019bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-9 highcharts-series-19 highcharts-legend-item-hidden"]')
    year_2019bc.click()
    time.sleep(1)
    year_loop('2019')
    year_2019ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-9 highcharts-series-19 "]')
    year_2019ac.click()
    time.sleep(0.5)
    year_2020bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-0 highcharts-series-20 highcharts-legend-item-hidden"]')
    year_2020bc.click()
    time.sleep(1)
    year_loop('2020')
    year_2020ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-0 highcharts-series-20 "]')
    year_2020ac.click()
    time.sleep(0.5)
    year_2021bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-1 highcharts-series-21 highcharts-legend-item-hidden"]')
    year_2021bc.click()
    time.sleep(1)
    year_loop('2021')
    year_2021ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-1 highcharts-series-21 "]')
    year_2021ac.click()
    time.sleep(0.5)
    year_2022bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-2 highcharts-series-22 highcharts-legend-item-hidden"]')
    year_2022bc.click()
    time.sleep(1)
    year_loop('2022')
    year_2022ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-2 highcharts-series-22 "]')
    year_2022ac.click()
    time.sleep(0.5)
    year_2023bc=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-3 highcharts-series-23 highcharts-legend-item-hidden"]')
    year_2023bc.click()
    time.sleep(1)
    year_loop('2023')
    year_2023ac=driver.find_element(By.XPATH,'//*[@class="highcharts-legend-item highcharts-line-series highcharts-color-3 highcharts-series-23 "]')
    year_2023ac.click()
    time.sleep(0.5)        
    location.clear()
    time.sleep(0.5)    
    chart_x=driver.find_element(By.XPATH,'//*[@id="chart-div"]/div[1]/button').click()
    time.sleep(1)    

#creating dataframe of the data and exporting it to excel file
df1=pd.DataFrame()
df1['State']=State
df1['District']=District
df1['Year']=current_year
df1['Dates']=Dates
df1['NDVI values']=NDVI_value
df1.drop_duplicates(inplace=True)
df1.to_excel(path+ State[0]+ '_' + 'NDVI Vedas.xlsx', index=False)
driver.close()
