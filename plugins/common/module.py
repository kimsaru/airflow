import os
import requests
from bs4 import BeautifulSoup

def get_dir_list(dir):
    str_list = ""

    if os.path.exists(dir):

        file_list = os.listdir(dir)
        file_list.sort()

        for f in file_list:
            full_path = os.path.join(dir, f)

            if os.path.isdir(full_path):
                f = "[" + f + "]" 
            str_list += f
            str_list += "\n"
        
        str_list.strip()
        return str_list
    
def get_weather(where):
    # div.status_wrap> div._today
    url = "https://search.naver.com/search.naver?query={}+날씨".format(where)
    r = requests.get(url)
    bs = BeautifulSoup(r.text, "lxml")

    w_box = bs.select("div.status_wrap> div._today")
    # print(w_box)

    if len(w_box) > 0:
        temperature = bs.select("div.temperature_text strong")
        
        print(temperature[0].text)
        print(temperature[1].text)
        print(temperature[2].text)



get_weather("서울")
