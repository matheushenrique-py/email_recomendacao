from datetime import datetime
import pytz
import requests
import pandas as pd
import json

data_fuso = pytz.timezone('America/Recife').localize(datetime.now())

with open('data/bases/header_api.txt', 'r', encoding='utf-8') as file:
    header = json.load(file)
url = 'http://transacional-apiv2.allin.com.br/api/report/?'
dt_envio = 'dt_envio=2020-04-09'
nm_envio = 'nm_envio=envio farma'


r = requests.get(url+'&'+dt_envio, headers=header)
resposta = r.json()

abertura = []
status = []
email = []
pag_erros = []
for i in range(1, resposta['last_page']+1):
    r = requests.get(url + dt_envio + '&' + nm_envio + '&' + 'page='+str(i), headers=header)
    try:
        resposta = r.json()

        print('Página ' + str(i))

        for i in range(len(resposta['data'])):
            status.append(resposta['data'][i]['status_msg'])
            abertura.append(resposta['data'][i]['dt_abertura'])
            email.append(resposta['data'][i]['nm_email'])
    except:
        pag_erros.append(i)


df = pd.DataFrame({'status': status, 'abertura': abertura, 'email': email})
df.to_csv('data/relatorios/relatorio_'+data_fuso.strftime('%Y-%m-%d')+'.csv', sep=',', index=False, encoding='utf-8')
print(pag_erros)