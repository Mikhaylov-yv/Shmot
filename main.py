import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import vk_api
from my_data import MyVKData_O

gneral_url = 'https://www.lamoda.ru'
url = '/c/1947/clothes-sports-myzsk-kurtki/?is_sale=1&page='
fil_name = 'Куртки.xlsx'
columns=['Наименование', 'Фирма', 'Цена','Скидка', 'Ссылка', 'Размеры']
df_new = pd.DataFrame([],columns = columns)
# Цена
def rasch_price(data):
    price = {}
    for pr in  data.find('span', class_ = 'price').find_all('span'):
        if ''.join(pr['class']) == 'price__old': price[''.join(pr['class'])] = pr.text.replace(' ', '')
        if ''.join(pr['class']) == 'price__new':
            price[''.join(pr['class'])] = pr.text.replace(' ', '')
            return [price['price__new'], 1-int(price['price__new'])/int(price['price__old'])]
        if ''.join(pr['class']) == 'price__new': price[''.join(pr['class'])] = pr.text.replace(' ', '')

exitFlag = False
for i in range(55):
    if exitFlag == True:
        print(i)
        break
    try:
        successful = False
        while not successful:
            pars_datd = []
            list_str = i + 1
            url_gen = gneral_url + url + str(list_str)
            print(url_gen)
            html = requests.get(url_gen)
            soup = BeautifulSoup(html.text, features='lxml')
            ad_data = soup.find_all('div', class_='products-list-item')
            if len(ad_data) <1:
                exitFlag = True
                break
            try:
                firm = ad_data[0].find('span', class_='products-list-item__brand-name').text
                successful = True
            except:
                successful = False
                # print(ad_data[0])
                time.sleep(2)
                continue
            for data in ad_data:
                firm = data.find('span', class_='products-list-item__brand-name').text
                name = data.find('span', class_='products-list-item__type').text
                if 'пуховик' not in name.lower():
                    continue
                tov_url = gneral_url + data.find('a', class_='products-list-item__link link')['href']
                price = rasch_price(data)
                sizes_data = []
                sizes = data.find('div', 'products-list-item__sizes').find_all('a')
                for siz in sizes:
                    sizes_data.append(siz.text)
                if '48' not in sizes_data and '50' not in '; '.join(sizes_data):
                    continue
                pars_datd.append([name, firm, price[0],price[1], tov_url,'; '.join(sizes_data)])
        print(pars_datd)
        df = pd.DataFrame(pars_datd,columns = columns)
        print(df)
        time.sleep(2)
        if df_new.shape[0] == 0:
            df_new = df
        else: df_new = pd.concat([df,df_new])
        print(df_new.shape)
    except:
        break
df_isx = pd.read_excel(fil_name)

df_isx.to_pickle('df_isx.pkl')
df_new.to_pickle('df_new.pkl')

# Фильтруем только новое или с меньшей ценой

df_nev_tov = df_new[~df_new['Ссылка'].isin(df_isx['Ссылка'].values)]
df_nev_cen = df_new[df_new.apply(lambda row: row['Ссылка'] in (df_isx['Ссылка'].values), axis=1)]
if df_nev_cen.shape[0]>0:
    df_nev_cen = df_nev_cen[df_nev_cen.apply(lambda row: int(row['Цена']) <  int(df_isx[df_isx['Ссылка'] == row['Ссылка']]['Цена'].min()), axis=1)]
    df_nev_fiilt = pd.concat([df_nev_cen,df_nev_tov])
else: df_nev_fiilt = df_nev_tov
print('Найдено новых записей: ' + str(df_new.shape[0]))
print('Осталось после фильтации: ' + str(df_nev_fiilt.shape[0]))
df_out = pd.concat([df_isx,df_nev_fiilt])
df_out = df_out.drop_duplicates()
df_out.to_excel(fil_name, index=False)
def wall_post(text,url,vk):
    v = 5.92
    time.sleep(5)
    vk.wall.post(owner_id='-187858877',
                 message=text,
                 attachment= url,v=v)

vk_session = vk_api.VkApi(token=MyVKData_O.TOKEN)
vk = vk_session.get_api()
vk_session_wall = vk_api.VkApi(login=MyVKData_O.LOGIN, password=MyVKData_O.GET_PASSWORD)
vk_session_wall.auth()
vk_wall = vk_session_wall.get_api()
print(df_nev_fiilt.shape[0])
for tov in  df_nev_fiilt.values.tolist():
    wall_post('Новая скидка на пуховик фирмы ' + str(tov[1]) +  ' цена: ' +str(tov[2]), tov[4], vk_wall)
    time.sleep(1)


