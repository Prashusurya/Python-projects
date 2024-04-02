
import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service

service = Service()
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=service, options=options)
driver.get("https://www.makemytrip.com/hotels/hotel-listing/?checkin=12072023&checkout=12082023&locusId=CTBOM&locusType=city&city=CTBOM&country=IN&searchText=Mumbai&roomStayQualifier=2e0e&_uCurrency=INR&reference=hotel&type=city&rsc=1e2e0e")
driver.maximize_window()
time.sleep(10)

deals_found=driver.find_element(By.XPATH, '//h1[@class="font24 latoBlack whiteText appendTop10"]')
print(deals_found.text)

driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
time.sleep(5)

screen_height = driver.execute_script("return window.screen.height;")
# print("Screen Height:",screen_height)

k=1
while True:
        k+=1
        try:
            driver.execute_script("window.scrollBy(0,1000)")
            time.sleep(1)
            scroll_height = driver.execute_script("return document.body.scrollHeight;")
            # print("Scroll Height:", scroll_height)
            driver.execute_script("window.scrollTo(0, {screen_height}*{k});".format(screen_height=screen_height, k=k))
            time.sleep(1)
            if(screen_height) * k > scroll_height:
                break            
        except:
            pass
time.sleep(5)

hotel_name=driver.find_elements(By.XPATH,'//*[@id="hlistpg_hotel_name"]')

hotel_rating=driver.find_elements(By.XPATH, '//*[@id="hlistpg_hotel_user_rating"]')

no_of_rating=driver.find_elements(By.XPATH, '//*[@class="font14 darkGreyText appendTop5"]')

hotel_price=driver.find_elements(By.XPATH,'//*[@id="hlistpg_hotel_shown_price"]')

locality=driver.find_elements(By.XPATH, '//*[@class="blueText"]')

hotel_names=[]
hotel_prices=[]
locality_all=[]
hotel_ratings=[]
no_of_ratings=[]

for each_hotel in hotel_name:
    hotel_names.append(each_hotel.text)  

for each_hotel_rating in hotel_rating:
    hotel_ratings.append(each_hotel_rating.text)
    
for each_no_rating in no_of_rating:
    no_of_ratings.append(each_no_rating.text)

for each_price in hotel_price:
    hotel_prices.append(each_price.text)
    
for each_locality in locality:
    locality_all.append(each_locality.text)
    
time.sleep(5)

df=pd.DataFrame({"Hotel Name":hotel_names, "Locality":locality_all, "Price":hotel_prices, "Hotel rating":hotel_ratings, "No. of hotel ratings":no_of_ratings})
df=pd.DataFrame({"Hotel Name":hotel_names, "Locality":locality_all, "Price":hotel_prices})
df1=pd.DataFrame({"hotel rating":hotel_ratings, "total ratings":no_of_ratings})
driver.close()
