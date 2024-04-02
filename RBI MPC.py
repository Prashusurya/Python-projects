
import pandas as pd
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import nltk
nltk.download('punkt')

options = Options()
options.add_argument("start-maximized")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://www.rbi.org.in/scripts/Annualpolicy.aspx")
time.sleep(3)

year_2016_17=driver.find_element(By.XPATH,'//*[@id="2024"]')
year_2016_17.click()
time.sleep(1)

mpc_all=driver.find_elements(By.PARTIAL_LINK_TEXT,'Minutes of the Monetary Policy Committee Meeting')
time.sleep(1)
links=[]
for each_mpc in mpc_all:
    links.append(each_mpc.get_attribute('href'))

for link in links:
    content=[]
    driver.get(link)
    time.sleep(3)
    file_name=driver.find_element(By.XPATH,'//*[@id="doublescroll"]/table[2]/tbody/tr[3]/td/b').text    
# headings=driver.find_elements(By.XPATH,"//p[@class='head']")
    paras=driver.find_elements(By.XPATH,'//*[@class="tablecontent1"]//p')

    for para in paras:
        content.append(para.text)
    df=pd.DataFrame()
    df['Content']=content            
    time.sleep(3)
    df.to_excel(file_name+'.xlsx',index=False)
    
    sentences = []

    for row in df['Content']:
        tokenized_sentences = nltk.sent_tokenize(row)        
        sentences.extend(tokenized_sentences)
    df1 = pd.DataFrame(sentences, columns=['Sentences'])
    df1.to_excel(file_name+" "+'sentences.xlsx', index=False)
time.sleep(2)    
driver.close()

