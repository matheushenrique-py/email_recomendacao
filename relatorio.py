from datetime import datetime
import pytz
import requests
import pandas as pd

data_fuso = pytz.timezone('America/Recife').localize(datetime.now())

header = {
  "Accept": "application/json",
  "Content-Type": "application/json",
  "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjRhMjNkYTg5ZmIxODI2NmE3NzYxZjY4ZmVhNDg0OTFiNzA5MWI5N2Q2N2EwMGY1YjU0N2E3M2FlYmE0MGY1ZWVhZTc0MGJkNjhkYzg4ZjM0In0.eyJhdWQiOiIyODYiLCJqdGkiOiI0YTIzZGE4OWZiMTgyNjZhNzc2MWY2OGZlYTQ4NDkxYjcwOTFiOTdkNjdhMDBmNWI1NDdhNzNhZWJhNDBmNWVlYWU3NDBiZDY4ZGM4OGYzNCIsImlhdCI6MTU4NTE0MzcwNywibmJmIjoxNTg1MTQzNzA3LCJleHAiOjE1ODY0Mzk3MDcsInN1YiI6IjU5NDYiLCJzY29wZXMiOltdfQ.jSexy_3znOcLWcsckW24YzCZzgorbmBV0T2cDW8jvDgOV6jJ7xtgNsqPDzKIZ9TFdhcBzkmEHaK5lyhWokfgaNXnO9WWIkdz2dIzbTs52jfhoM2UKuZPUfO6nkHBbq8SiFwYJFyuwnH-eY8DbBYBuFeSdZK0n9fRelJqtkBnVYNtm_88TnpoeJiFsZwtyb1R7eDsQ-HyuuIWda6kNVsikEs94_U36rqxkvyqbUPqO_DwMZRLcfvaQ7aqLYSgicMTnv6B_-v46kKCkdFCQ3EfvnPPCTsBh05_S0y8ImRvW7L0frhLGXMn_83ZPAAOlK5e8lDM8egcwgGpqHbu3lWDiJjZBwDe9LsGyWXjS4fhgYWSc49sSQvP0QoNNoUmaVl5a3YYT_UuDsBWNkpL1tX4egmGaQIgxtlE2cBhpA5vnr03yeYDwZkbcepDVh--J-GsOLY5gAgXvRydjp4YfBnjXffeVfiTDQNSFFs8mxAnX0Iq9CMcjzDxv58J64gQ9Ara6N9EDUfdh4cvQrrOwMRwwsKH_hrZcPGIBdm4LqIXMQxwYsSzQ95BgWPspl2ad9aFFC-pGaTDcIB-sWMN6BuzWErsNSrhLYl_TLUl7cMO7mBq7Jt-qZssMHoQxWxqKYBqsCzR_fuQRye6ZD4K6i8m2VS1zZLdAk4yryC8yhbJuOQ"
}
url = 'http://transacional-apiv2.allin.com.br/api/report/?'
dt_envio = 'dt_envio=2020-03-31'
nm_envio = 'nm_envio=Recomendação de produtos oficial'


r = requests.get(url+dt_envio+'&'+nm_envio, headers=header)
resposta = r.json()

abertura = []
status = []
email = []
pag_erros = []
for i in range(1, resposta['last_page']+1):
    r = requests.get(url + dt_envio + '&' + nm_envio+'&'+'page='+str(i), headers=header)
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