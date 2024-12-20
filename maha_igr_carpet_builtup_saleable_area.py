import pandas as pd
import re
from sqlalchemy import create_engine
from urllib.parse import quote
import urllib3
import unicodedata
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
raw = create_engine("mysql://altrr_db:%s@192.168.0.20:1234/altrr_database"% quote("abcd"))
engine2 = create_engine("mysql://altrr_db:%s@192.168.0.32:1234/processing"% quote("abcd"))
query="SELECT Id,Property_description FROM  altrr_database.tbl_maharashtra_igr_basic"
df=pd.read_sql(query, raw)

done=pd.read_sql("select Id from tbl_maha_all_area_final", engine2)

df_new=df[~df['Id'].isin(done['Id'])]

df1=df_new.copy()

def bfl_area(description):
    text =description.replace('चौ.',' चौ ').replace('क्षेत्रफळ',' क्षेत्र ').replace('क्षेत्र.', ' क्षेत्र ').replace('मजल्यावरील',' क्षेत्र ').replace(' .','.').replace('. ','.')
    new_texs = text.replace('कारपेट', ' कारपेट ').replace('कार्पेट', ' कार्पेट ').replace('कार्पेटएकूण', ' कार्पेटएकूण ').replace('बिल्टअप' , ' बिल्टअप ').replace('बिल्ट अप', ' बिल्ट अप ').replace('बिल्टअप', ' बिल्टअप ').replace('बांधीव', ' बांधीव ').replace('बांधीव.', ' बांधीव. ')
    marker = "चौ"
    words = new_texs.split()
    results = []
    for i, word in enumerate(words):
        if marker in word:
            before_values = words[max(0, i - 3):i]
            after_values = words[i + 1:i + 4] 
            result = ' '.join(before_values + [marker] +after_values)
            results.append(result)
    joined_address = ' :::;;::: '.join(results)
    return joined_address

df1['area_all_info']=df1['Property_description'].apply(bfl_area)
 
carpet_area = ['कारपेट', 'कार्पेट', 'कार्पेटएकूण']
built_up_area = ['बिल्टअप', 'बिल्ट अप', 'बिल्टअप']
saleable_area = ['सेलेबल']
print('step 1 compl')
def check_area_type(text):
    """Check the type of area based on keywords."""
    if any(keyword in text for keyword in carpet_area):
        return "Carpet Area"
    elif any(keyword in text for keyword in built_up_area):
        return "Built-up Area"
    elif any(keyword in text for keyword in saleable_area):
        return "Saleable Area"
    return "No Area Type Found"

def bfl_area_2(description):
    """Extract area type and value from the given description."""
    words = description.split(':::;;:::')
    for i in words:
        result = check_area_type(i)
        if result != "No Area Type Found":
            match_w = re.search(r'(\d+(\.\d+)?)\s+चौ\s+(\S+)', i)
            if match_w:
                number1 = match_w.group(1)
                next_word = match_w.group(3)
                area_sq_feet = None
                
                # Convert square meters to square feet if necessary
                if 'मी' in next_word or 'मि' in next_word:
                    try:
                        area_sq_meters = eval(number1)
                        area_sq_feet = int(area_sq_meters * 10.7639)
                    except Exception as e:
                        print(f'Error in conversion: {e}')
                        area_sq_feet = number1
                else:
                    area_sq_feet = number1
                
                return area_sq_feet, result
    
    return None, None


area_results = df1['area_all_info'].apply(bfl_area)
area_results = df1['area_all_info'].apply(bfl_area_2)
# Check the lengths of area_results
if area_results.apply(len).nunique() != 1:
    print("Mismatch in lengths of results. Please check the bfl_area function.")

# Split the tuples into separate columns
df1[['Chargable_area', 'Carpet_area']] = pd.DataFrame(area_results.tolist(), index=df1.index)
df2=df1[['Id', 'Property_description','Chargable_area', 'Carpet_area']]
print('step 3 compl')
df2.dropna(subset=['Chargable_area', 'Carpet_area'], inplace=True)
df2=df2.drop('Property_description',axis=1)
df2.to_sql('tbl_maha_all_area_final', con=engine2, if_exists='append', index=False)
print('step 4 compl')