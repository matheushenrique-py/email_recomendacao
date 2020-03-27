from datetime import datetime, date
import requests
import xml.etree.ElementTree as ET

import pandas as pd
import numpy as np

#from data.bmldev.loads import txts_to_pd

estoque = pd.read_csv('data/bases/sku_dispo.csv', encoding='ISO-8859-1')
estoque['situacao'] = [False if x=='INDISPONÃ\x8dVEL!' else True for x in estoque['situacao']]

'''
# a priori eu não preciso dessa lista de clientes, já tenho os emails necessários na recomendação
email15 = pd.read_csv('Email 0 a 15mil.csv', encoding='ISO-8859-1')
email15.columns = ['Cliente', 'Nome', 'Flag', 'Email']
email15['Email'] = email15['Email'].apply(lambda x: x.replace(' ',''))
email18 = txts_to_pd(['Email 15001 a 18mil.txt'], 4)
email18.columns = ['Cliente', 'Nome', 'Flag', 'Email']
'''

recom15 = pd.read_csv('data/bases/Recom 0 a 15mil.csv', encoding='ISO-8859-1')
recom33 = pd.read_csv('data/bases/Recom 15001 a 33mil.csv', encoding='ISO-8859-1')
recom15.drop(columns='Unnamed: 0', inplace=True)
recom33.columns = recom15.columns
recom = pd.concat([recom15, recom33])

recom.replace('#N/DISP', np.NaN, inplace=True)
recom.dropna(subset=['E-mail sap'], axis=0,  inplace=True)

recom.columns = ['clientCod', 'Nome', 'UF', 'TEL', 'Cidade',
                'Bairro', 'Dt_aniv', 'Email', 'CodeProd', 'Product',
                'CodeRecom', 'Estoque', 'Recomendations', 'confidence', 'data',
                'fatCod']

recom['Email'] = recom['Email'].apply(lambda x: x.replace(' ', ''))
recom['Estoque'] = recom['Estoque'].apply(lambda x: float(x))
recom.drop(index=recom['Estoque'][recom['Estoque'] > 5].index, inplace=True)

recomendacoes = []
for pessoa in recom['Nome'].unique():
    recomendacoes.append(recom['CodeRecom'][recom['Nome'] == pessoa].values.tolist())

for i in range(len(recomendacoes)):
    for j in range(2):
        try:
          recomendacoes[i,j]
        except:
          recomendacoes[i].append(np.NaN)
        recomendacoes[i] = recomendacoes[i][0:3]

df = pd.DataFrame(data=recomendacoes, index=recom['clientCod'].unique(), columns=['Recom1', 'Recom2', 'Recom3'])
df.index.name = 'Cliente'
df = df.merge(recom.loc[:,['clientCod','Email']].drop_duplicates(), left_on='Cliente', right_on='clientCod')
df_teste = pd.read_csv('data/bases/lista_testes.csv')

# request para acessar lista de produtos da bol
url_xml = "https://www.bemol.com.br/feeds/google-merchant"
header = { "Accept": "application/xml" }
r = requests.get(url_xml, headers=header)

# Gera um DataFrame com os produtos da BOL
tree =  ET.ElementTree(ET.fromstring(r.content))
root = tree.getroot()

codigos, produtos, images, links, precos = ([] for i in range(5))
for channel in root.findall("channel"):
    for item in channel.findall("item"):
        for sem_imagem in item.findall("{http://base.google.com/ns/1.0}image_link"):
            sem_imagem.tag = "{http://base.google.com/ns/1.0}additional_image_link"
        for title in item.findall("title"):
            produtos.append(title.text.capitalize())
        for image in item.findall("{http://base.google.com/ns/1.0}additional_image_link"):
            images.append(image.text)
        for link in item.findall("link"):
            links.append(link.text)
        for preco in item.findall("{http://base.google.com/ns/1.0}price"):
            precos.append(float(preco.text.replace(" BRL", "")))
        for codigo in item.findall("{http://base.google.com/ns/1.0}mpn"):
            codigos.append(int(codigo.text))

df_produtos = pd.DataFrame({'Nome': produtos, 'Imagem': images, 'Link': links, 'Preco': precos}, index=codigos)

# requisição para a API
url = "http://transacional-apiv2.allin.com.br/api/email/bulk"

emails_teste = ['matheusamorim@bemol.com.br', 'matheus.amorim@outlook.pt']

headers = {
  "Accept": "application/json",
  "Content-Type": "application/json",
  "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjRhMjNkYTg5ZmIxODI2NmE3NzYxZjY4ZmVhNDg0OTFiNzA5MWI5N2Q2N2EwMGY1YjU0N2E3M2FlYmE0MGY1ZWVhZTc0MGJkNjhkYzg4ZjM0In0.eyJhdWQiOiIyODYiLCJqdGkiOiI0YTIzZGE4OWZiMTgyNjZhNzc2MWY2OGZlYTQ4NDkxYjcwOTFiOTdkNjdhMDBmNWI1NDdhNzNhZWJhNDBmNWVlYWU3NDBiZDY4ZGM4OGYzNCIsImlhdCI6MTU4NTE0MzcwNywibmJmIjoxNTg1MTQzNzA3LCJleHAiOjE1ODY0Mzk3MDcsInN1YiI6IjU5NDYiLCJzY29wZXMiOltdfQ.jSexy_3znOcLWcsckW24YzCZzgorbmBV0T2cDW8jvDgOV6jJ7xtgNsqPDzKIZ9TFdhcBzkmEHaK5lyhWokfgaNXnO9WWIkdz2dIzbTs52jfhoM2UKuZPUfO6nkHBbq8SiFwYJFyuwnH-eY8DbBYBuFeSdZK0n9fRelJqtkBnVYNtm_88TnpoeJiFsZwtyb1R7eDsQ-HyuuIWda6kNVsikEs94_U36rqxkvyqbUPqO_DwMZRLcfvaQ7aqLYSgicMTnv6B_-v46kKCkdFCQ3EfvnPPCTsBh05_S0y8ImRvW7L0frhLGXMn_83ZPAAOlK5e8lDM8egcwgGpqHbu3lWDiJjZBwDe9LsGyWXjS4fhgYWSc49sSQvP0QoNNoUmaVl5a3YYT_UuDsBWNkpL1tX4egmGaQIgxtlE2cBhpA5vnr03yeYDwZkbcepDVh--J-GsOLY5gAgXvRydjp4YfBnjXffeVfiTDQNSFFs8mxAnX0Iq9CMcjzDxv58J64gQ9Ara6N9EDUfdh4cvQrrOwMRwwsKH_hrZcPGIBdm4LqIXMQxwYsSzQ95BgWPspl2ad9aFFC-pGaTDcIB-sWMN6BuzWErsNSrhLYl_TLUl7cMO7mBq7Jt-qZssMHoQxWxqKYBqsCzR_fuQRye6ZD4K6i8m2VS1zZLdAk4yryC8yhbJuOQ"
}
template = "PGh0bWw+CjwhLS0gSFRNTCA0IC0tPgo8bWV0YSBodHRwLWVxdWl2PSJDb250ZW50LVR5cGUiIGNvbnRlbnQ9InRleHQvaHRtbDsgY2hhcnNldD11dGYtOCI+CjwhLS0gSFRNTDUgLS0+CjxtZXRhIGNoYXJzZXQ9InV0Zi04Ij4KPGhlYWQ+CiAgICAKPC9oZWFkPgo8Ym9keSBzdHlsZT0iZm9udC1mYW1pbHk6IG15cmlhZC1wcm8sIHNhbnMtc2VyaWY7IGZvbnQtd2VpZ2h0OiA0MDA7IGZvbnQtc3R5bGU6IG5vcm1hbDsgYmFja2dyb3VuZC1jb2xvcjogI2VlZWVlZTsiPgogICAgPGRpdiBjbGFzcz0iY29ycG8iIHN0eWxlPSJtYXgtd2lkdGg6IDcwMHB4OyBtYXJnaW46IGF1dG87IGRpc3BsYXk6IGJsb2NrOyBvdmVyZmxvdzogYXV0bzsgYmFja2dyb3VuZC1jb2xvcjogI2ZmZmZmZjsiPgogICAgICAgIDxoZWFkZXI+CiAgICAgICAgICAgIDxpbWcgc3JjPSJodHRwczovL2dpdGh1Yi5jb20vbWF0aGV1c2hlbnJpcXVlLXB5L2VtYWlsX3JlY29tZW5kYWNhby9yYXcvbWFzdGVyL2RhdGEvY2FiZWNhbGhvLnBuZyIgLD0iIiBhbHQ9ImxvZ28gTG9qYXMgQmVtb2wiIGNsYXNzPSJjYWJlY2FsaG8iIHN0eWxlPSJtYXgtd2lkdGg6IDEwMCU7IGhlaWdodDogYXV0bzsgb2JqZWN0LWZpdDogc2NhbGUtZG93bjsiPgogICAgICAgIDwvaGVhZGVyPgoKICAgICAgICA8ZGl2IGNsYXNzPSJyZWNvbWVuZGFjYW8iIHN0eWxlPSJ3aWR0aDogY2FsYygxMDAlIC0gNDBweCk7IGhlaWdodDogMjgwcHg7IG1hcmdpbjogMjBweDsgZGlzcGxheTogYmxvY2s7Ij4KICAgICAgICAgICAgPGRpdiBjbGFzcz0iZXNxdWVyZGEiIHN0eWxlPSJ3aWR0aDogNTAlOyB0ZXh0LWFsaWduOiBsZWZ0OyBmbG9hdDogbGVmdDsgZGlzcGxheTogaW5saW5lOyI+CiAgICAgICAgICAgICAgICA8aW1nIHNyYz0ie3tpbWdfcHJvZHV0bzF9fSIgLD0iIiBjbGFzcz0iaW1nX3Byb2R1dG8iIGFsdD0ie3tub21lX3Byb2R1dG8xfX0iIHN0eWxlPSJ3aWR0aDogOTglOyBtYXgtaGVpZ2h0OiAyMDBweDsgb2JqZWN0LWZpdDogc2NhbGUtZG93bjsiPgogICAgICAgICAgICA8L2Rpdj4KICAgICAgICAgICAgPGRpdiBjbGFzcz0iZGlyZWl0YSIgc3R5bGU9IndpZHRoOiA1MCU7IHRleHQtYWxpZ246IHJpZ2h0OyBmbG9hdDogcmlnaHQ7IGRpc3BsYXk6IGlubGluZTsiPgogICAgICAgICAgICAgICAgPGVtIHN0eWxlPSJmb250LXN0eWxlOiBub3JtYWw7IGZvbnQtd2VpZ2h0OiBib2xkOyBmb250LXNpemU6IDIzcHg7IGRpc3BsYXk6IGJsb2NrOyBtYXJnaW4tdG9wOiAxMHB4OyI+e3tub21lX3Byb2R1dG8xfX08L2VtPgogICAgICAgICAgICAgICAgPHN0cm9uZyBzdHlsZT0iY29sb3I6ICNkZDM5Mzc7IGZvbnQtc2l6ZTogMzJweDsgZGlzcGxheTogYmxvY2s7IHRleHQtdHJhbnNmb3JtOiBjYXBpdGFsaXplOyI+UiQge3twcmVjb19wcm9kdXRvMX19PC9zdHJvbmc+CiAgICAgICAgICAgICAgICA8c21hbGwgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBjb2xvcjogIzY2NjY2NjsgZm9udC1zaXplOiAxNHB4OyBmb250LXdlaWdodDogMTAwOyBkaXNwbGF5OiBibG9jazsiPjR4IGRlIFIkIHt7cGFyY2VsYV9wcm9kdXRvMX19IG5vIENhcnTDo28gQmVtb2w8L3NtYWxsPgogICAgICAgICAgICAgICAgPGEgaHJlZj0ie3tsaW5rX3Byb2R1dG8xfX0iICw9IiIgY2xhc3M9ImJ0X2NvbXByYXIiIHN0eWxlPSJmb250LWZhbWlseTogc2Fucy1zZXJpZjsgZmxvYXQ6IGNlbnRlcjsgZGlzcGxheTogaW5saW5lLWJsb2NrOyB3aWR0aDogNTAlOyBwYWRkaW5nOiAxMHB4IDUlOyBtYXJnaW4tdG9wOiAxMHB4OyBiYWNrZ3JvdW5kLWNvbG9yOiAjZGQzOTM3OyBmb250LXNpemU6IDE2cHg7IGZvbnQtd2VpZ2h0OiBib2xkOyBjb2xvcjogI2ZmZmZmZjsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyB0ZXh0LWFsaWduOiBjZW50ZXI7IGJvcmRlci1yYWRpdXM6IDEwcHg7Ij5DT01QUkFSPC9hPgogICAgICAgICAgICA8L2Rpdj4KICAgICAgICA8L2Rpdj4KCiAgICAgICAgPGRpdiBjbGFzcz0icmVjb21lbmRhY2FvIiBzdHlsZT0id2lkdGg6IGNhbGMoMTAwJSAtIDQwcHgpOyBoZWlnaHQ6IDI4MHB4OyBtYXJnaW46IDIwcHg7IGRpc3BsYXk6IGJsb2NrOyI+CiAgICAgICAgICAgIDxkaXYgY2xhc3M9ImVzcXVlcmRhIiBzdHlsZT0id2lkdGg6IDUwJTsgdGV4dC1hbGlnbjogbGVmdDsgZmxvYXQ6IGxlZnQ7IGRpc3BsYXk6IGlubGluZTsiPgogICAgICAgICAgICAgICAgPGVtIHN0eWxlPSJmb250LXN0eWxlOiBub3JtYWw7IGZvbnQtd2VpZ2h0OiBib2xkOyBmb250LXNpemU6IDIzcHg7IGRpc3BsYXk6IGJsb2NrOyBtYXJnaW4tdG9wOiAxMHB4OyI+e3tub21lX3Byb2R1dG8yfX08L2VtPgogICAgICAgICAgICAgICAgPHN0cm9uZyBzdHlsZT0iY29sb3I6ICNkZDM5Mzc7IGZvbnQtc2l6ZTogMzJweDsgZGlzcGxheTogYmxvY2s7IHRleHQtdHJhbnNmb3JtOiBjYXBpdGFsaXplOyI+UiQge3twcmVjb19wcm9kdXRvMn19PC9zdHJvbmc+CiAgICAgICAgICAgICAgICA8c21hbGwgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBjb2xvcjogIzY2NjY2NjsgZm9udC1zaXplOiAxNHB4OyBmb250LXdlaWdodDogMTAwOyBkaXNwbGF5OiBibG9jazsiPjR4IGRlIFIkIHt7cGFyY2VsYV9wcm9kdXRvMn19IG5vIENhcnTDo28gQmVtb2w8L3NtYWxsPgogICAgICAgICAgICAgICAgPGEgaHJlZj0ie3tsaW5rX3Byb2R1dG8yfX0iICw9IiIgY2xhc3M9ImJ0X2NvbXByYXIiIHN0eWxlPSJmb250LWZhbWlseTogc2Fucy1zZXJpZjsgZmxvYXQ6IGNlbnRlcjsgZGlzcGxheTogaW5saW5lLWJsb2NrOyB3aWR0aDogNTAlOyBwYWRkaW5nOiAxMHB4IDUlOyBtYXJnaW4tdG9wOiAxMHB4OyBiYWNrZ3JvdW5kLWNvbG9yOiAjZGQzOTM3OyBmb250LXNpemU6IDE2cHg7IGZvbnQtd2VpZ2h0OiBib2xkOyBjb2xvcjogI2ZmZmZmZjsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyB0ZXh0LWFsaWduOiBjZW50ZXI7IGJvcmRlci1yYWRpdXM6IDEwcHg7Ij5DT01QUkFSPC9hPgogICAgICAgICAgICA8L2Rpdj4KICAgICAgICAgICAgPGRpdiBjbGFzcz0iZGlyZWl0YSIgc3R5bGU9IndpZHRoOiA1MCU7IHRleHQtYWxpZ246IHJpZ2h0OyBmbG9hdDogcmlnaHQ7IGRpc3BsYXk6IGlubGluZTsiPgogICAgICAgICAgICAgICAgPGltZyBzcmM9Int7aW1nX3Byb2R1dG8yfX0iICw9IiIgY2xhc3M9ImltZ19wcm9kdXRvIiBhbHQ9Int7bm9tZV9wcm9kdXRvMn19IiBzdHlsZT0id2lkdGg6IDk4JTsgbWF4LWhlaWdodDogMjAwcHg7IG9iamVjdC1maXQ6IHNjYWxlLWRvd247Ij4KICAgICAgICAgICAgPC9kaXY+CiAgICAgICAgPC9kaXY+CgogICAgICAgIDxkaXYgY2xhc3M9InJlY29tZW5kYWNhbyIgc3R5bGU9IndpZHRoOiBjYWxjKDEwMCUgLSA0MHB4KTsgaGVpZ2h0OiAyODBweDsgbWFyZ2luOiAyMHB4OyBkaXNwbGF5OiBibG9jazsiPgogICAgICAgICAgICA8ZGl2IGNsYXNzPSJlc3F1ZXJkYSIgc3R5bGU9IndpZHRoOiA1MCU7IHRleHQtYWxpZ246IGxlZnQ7IGZsb2F0OiBsZWZ0OyBkaXNwbGF5OiBpbmxpbmU7Ij4KICAgICAgICAgICAgICAgIDxpbWcgc3JjPSJ7e2ltZ19wcm9kdXRvM319IiAsPSIiIGNsYXNzPSJpbWdfcHJvZHV0byIgYWx0PSJ7e25vbWVfcHJvZHV0bzN9fSIgc3R5bGU9IndpZHRoOiA5OCU7IG1heC1oZWlnaHQ6IDIwMHB4OyBvYmplY3QtZml0OiBzY2FsZS1kb3duOyI+CiAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICA8ZGl2IGNsYXNzPSJkaXJlaXRhIiBzdHlsZT0id2lkdGg6IDUwJTsgdGV4dC1hbGlnbjogcmlnaHQ7IGZsb2F0OiByaWdodDsgZGlzcGxheTogaW5saW5lOyI+CiAgICAgICAgICAgICAgICA8ZW0gc3R5bGU9ImZvbnQtc3R5bGU6IG5vcm1hbDsgZm9udC13ZWlnaHQ6IGJvbGQ7IGZvbnQtc2l6ZTogMjNweDsgZGlzcGxheTogYmxvY2s7IG1hcmdpbi10b3A6IDEwcHg7Ij57e25vbWVfcHJvZHV0bzN9fTwvZW0+CiAgICAgICAgICAgICAgICA8c3Ryb25nIHN0eWxlPSJjb2xvcjogI2RkMzkzNzsgZm9udC1zaXplOiAzMnB4OyBkaXNwbGF5OiBibG9jazsgdGV4dC10cmFuc2Zvcm06IGNhcGl0YWxpemU7Ij5SJCB7e3ByZWNvX3Byb2R1dG8zfX08L3N0cm9uZz4KICAgICAgICAgICAgICAgIDxzbWFsbCBzdHlsZT0iZm9udC1mYW1pbHk6IHNhbnMtc2VyaWY7IGNvbG9yOiAjNjY2NjY2OyBmb250LXNpemU6IDE0cHg7IGZvbnQtd2VpZ2h0OiAxMDA7IGRpc3BsYXk6IGJsb2NrOyI+NHggZGUgUiQge3twYXJjZWxhX3Byb2R1dG8zfX0gbm8gQ2FydMOjbyBCZW1vbDwvc21hbGw+CiAgICAgICAgICAgICAgICA8YSBocmVmPSJ7e2xpbmtfcHJvZHV0bzN9fSIgLD0iIiBjbGFzcz0iYnRfY29tcHJhciIgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBmbG9hdDogY2VudGVyOyBkaXNwbGF5OiBpbmxpbmUtYmxvY2s7IHdpZHRoOiA1MCU7IHBhZGRpbmc6IDEwcHggNSU7IG1hcmdpbi10b3A6IDEwcHg7IGJhY2tncm91bmQtY29sb3I6ICNkZDM5Mzc7IGZvbnQtc2l6ZTogMTZweDsgZm9udC13ZWlnaHQ6IGJvbGQ7IGNvbG9yOiAjZmZmZmZmOyB0ZXh0LWRlY29yYXRpb246IG5vbmU7IHRleHQtYWxpZ246IGNlbnRlcjsgYm9yZGVyLXJhZGl1czogMTBweDsiPkNPTVBSQVI8L2E+CiAgICAgICAgICAgIDwvZGl2PgogICAgICAgIDwvZGl2PgoKICAgICAgICA8Zm9vdGVyPgogICAgICAgICAgICA8ZGl2IGNsYXNzPSJhdmlzb19hYmVydG8iIHN0eWxlPSJiYWNrZ3JvdW5kLWNvbG9yOiAjZmZjNjAwOyBwYWRkaW5nOiAyMHB4OyI+CiAgICAgICAgICAgICAgICA8aDEgc3R5bGU9InRleHQtYWxpZ246IGNlbnRlcjsgZm9udC1zaXplOiAyNHB4OyBmb250LXdlaWdodDogYm9sZDsiPkFzIGZhcm3DoWNpYXMgZXN0w6NvIGFiZXJ0YXMgcGFyYSBwYWdhbWVudG9zIGRlIHBhcmNlbGFzIGRvIGNhcm7DqiBCZW1vbCE8L2gxPgogICAgICAgICAgICAgICAgPGEgaHJlZj0iaHR0cHM6Ly93d3cuYmVtb2wuY29tLmJyL2hvcmFyaW9zLWVzcGVjaWFpcyIgLD0iIiBjbGFzcz0iYnRfaG9yYXJpb3MiIHN0eWxlPSJmb250LWZhbWlseTogc2Fucy1zZXJpZjsgZmxvYXQ6IGNlbnRlcjsgZGlzcGxheTogYmxvY2s7IG1heC13aWR0aDogNjAlOyBwYWRkaW5nOiAxMHB4IDIwcHg7IG1hcmdpbjogYXV0bzsgYmFja2dyb3VuZC1jb2xvcjogI2RkMzkzNzsgZm9udC1zaXplOiAxNnB4OyBmb250LXdlaWdodDogYm9sZDsgY29sb3I6ICNmZmZmZmY7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgdGV4dC1hbGlnbjogY2VudGVyOyBib3JkZXItcmFkaXVzOiAxMHB4OyBib3gtc2hhZG93OiAxcHggMXB4IDJweCAjM0Q0NTREOyI+Q09ORklSQSBPUyBIT1LDgVJJT1M8L2E+CiAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICA8ZGl2IGNsYXNzPSJjZW50cmFsX2F0ZW5kaW1lbnRvIiBzdHlsZT0iYmFja2dyb3VuZC1jb2xvcjogIzAwOTdkODsgcGFkZGluZzogMjBweCAwOyBkaXNwbGF5OiBibG9jazsiPgogICAgICAgICAgICAgICAgPGRpdiBjbGFzcz0ic29jaWFsX2Nvbmp1bnRvIiBzdHlsZT0icGFkZGluZy1sZWZ0OiAyMCU7IHBhZGRpbmctcmlnaHQ6IDIwJTsgZGlzcGxheTogYmxvY2s7Ij4KICAgICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPSJyZWRlc19zb2NpYWlzIiBzdHlsZT0id2lkdGg6IDE1JTsgZGlzcGxheTogaW5saW5lLWJsb2NrOyBtYXJnaW46IDIlOyI+CiAgICAgICAgICAgICAgICAgICAgICAgIDxhIGhyZWY9Imh0dHBzOi8vd3d3LmZhY2Vib29rLmNvbS9iZW1vbG9ubGluZSIgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBmb250LXdlaWdodDogMjAwOyBmb250LXNpemU6IDY4JTsgY29sb3I6ICNmZmZmZmY7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgdGV4dC1hbGlnbjogY2VudGVyOyBmbG9hdDogY2VudGVyOyBkaXNwbGF5OiBibG9jazsiPgogICAgICAgICAgICAgICAgICAgICAgICAgICAgPGltZyBzcmM9Imh0dHBzOi8vZ2l0aHViLmNvbS9tYXRoZXVzaGVucmlxdWUtcHkvZW1haWxfcmVjb21lbmRhY2FvL3Jhdy9tYXN0ZXIvZGF0YS9mYWNlYm9vay5wbmciICw9IiIgY2xhc3M9ImxvZ29fc29jaWFsIiBzdHlsZT0ibWFyZ2luOiBhdXRvOyBwYWRkaW5nLWJvdHRvbTogNXB4OyBkaXNwbGF5OiBibG9jazsgd2lkdGg6IDkwJTsgaGVpZ2h0OiBhdXRvOyBvYmplY3QtZml0OiBzY2FsZS1kb3duOyI+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICBmYWNlYm9vawogICAgICAgICAgICAgICAgICAgICAgICA8L2E+CiAgICAgICAgICAgICAgICAgICAgPC9kaXY+CiAgICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz0icmVkZXNfc29jaWFpcyIgc3R5bGU9IndpZHRoOiAxNSU7IGRpc3BsYXk6IGlubGluZS1ibG9jazsgbWFyZ2luOiAyJTsiPgogICAgICAgICAgICAgICAgICAgICAgICA8YSBocmVmPSJodHRwczovL3R3aXR0ZXIuY29tL0JlbW9sT25saW5lIiBzdHlsZT0iZm9udC1mYW1pbHk6IHNhbnMtc2VyaWY7IGZvbnQtd2VpZ2h0OiAyMDA7IGZvbnQtc2l6ZTogNjglOyBjb2xvcjogI2ZmZmZmZjsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyB0ZXh0LWFsaWduOiBjZW50ZXI7IGZsb2F0OiBjZW50ZXI7IGRpc3BsYXk6IGJsb2NrOyI+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICA8aW1nIHNyYz0iaHR0cHM6Ly9naXRodWIuY29tL21hdGhldXNoZW5yaXF1ZS1weS9lbWFpbF9yZWNvbWVuZGFjYW8vcmF3L21hc3Rlci9kYXRhL3R3aXR0ZXIucG5nIiAsPSIiIGNsYXNzPSJsb2dvX3NvY2lhbCIgc3R5bGU9Im1hcmdpbjogYXV0bzsgcGFkZGluZy1ib3R0b206IDVweDsgZGlzcGxheTogYmxvY2s7IHdpZHRoOiA5MCU7IGhlaWdodDogYXV0bzsgb2JqZWN0LWZpdDogc2NhbGUtZG93bjsiPgogICAgICAgICAgICAgICAgICAgICAgICAgICAgdHdpdHRlcgogICAgICAgICAgICAgICAgICAgICAgICA8L2E+CiAgICAgICAgICAgICAgICAgICAgPC9kaXY+CiAgICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz0icmVkZXNfc29jaWFpcyIgc3R5bGU9IndpZHRoOiAxNSU7IGRpc3BsYXk6IGlubGluZS1ibG9jazsgbWFyZ2luOiAyJTsiPgogICAgICAgICAgICAgICAgICAgICAgICA8YSBocmVmPSJodHRwczovL3d3dy5pbnN0YWdyYW0uY29tL2JlbW9sb25saW5lLyIgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBmb250LXdlaWdodDogMjAwOyBmb250LXNpemU6IDY4JTsgY29sb3I6ICNmZmZmZmY7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgdGV4dC1hbGlnbjogY2VudGVyOyBmbG9hdDogY2VudGVyOyBkaXNwbGF5OiBibG9jazsiPgogICAgICAgICAgICAgICAgICAgICAgICAgICAgPGltZyBzcmM9Imh0dHBzOi8vZ2l0aHViLmNvbS9tYXRoZXVzaGVucmlxdWUtcHkvZW1haWxfcmVjb21lbmRhY2FvL3Jhdy9tYXN0ZXIvZGF0YS9pbnN0YWdyYW0ucG5nIiAsPSIiIGNsYXNzPSJsb2dvX3NvY2lhbCIgc3R5bGU9Im1hcmdpbjogYXV0bzsgcGFkZGluZy1ib3R0b206IDVweDsgZGlzcGxheTogYmxvY2s7IHdpZHRoOiA5MCU7IGhlaWdodDogYXV0bzsgb2JqZWN0LWZpdDogc2NhbGUtZG93bjsiPgogICAgICAgICAgICAgICAgICAgICAgICAgICAgaW5zdGFncmFtCiAgICAgICAgICAgICAgICAgICAgICAgIDwvYT4KICAgICAgICAgICAgICAgICAgICA8L2Rpdj4KICAgICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPSJyZWRlc19zb2NpYWlzIiBzdHlsZT0id2lkdGg6IDE1JTsgZGlzcGxheTogaW5saW5lLWJsb2NrOyBtYXJnaW46IDIlOyI+CiAgICAgICAgICAgICAgICAgICAgICAgIDxhIGhyZWY9Imh0dHBzOi8vd3d3LmxpbmtlZGluLmNvbS9jb21wYW55L2JlbW9sLWZvZy1zIiBzdHlsZT0iZm9udC1mYW1pbHk6IHNhbnMtc2VyaWY7IGZvbnQtd2VpZ2h0OiAyMDA7IGZvbnQtc2l6ZTogNjglOyBjb2xvcjogI2ZmZmZmZjsgdGV4dC1kZWNvcmF0aW9uOiBub25lOyB0ZXh0LWFsaWduOiBjZW50ZXI7IGZsb2F0OiBjZW50ZXI7IGRpc3BsYXk6IGJsb2NrOyI+CiAgICAgICAgICAgICAgICAgICAgICAgICAgICA8aW1nIHNyYz0iaHR0cHM6Ly9naXRodWIuY29tL21hdGhldXNoZW5yaXF1ZS1weS9lbWFpbF9yZWNvbWVuZGFjYW8vcmF3L21hc3Rlci9kYXRhL2xpbmtlZGluLnBuZyIgLD0iIiBjbGFzcz0ibG9nb19zb2NpYWwiIHN0eWxlPSJtYXJnaW46IGF1dG87IHBhZGRpbmctYm90dG9tOiA1cHg7IGRpc3BsYXk6IGJsb2NrOyB3aWR0aDogOTAlOyBoZWlnaHQ6IGF1dG87IG9iamVjdC1maXQ6IHNjYWxlLWRvd247Ij4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIGxpbmtlZGluCiAgICAgICAgICAgICAgICAgICAgICAgIDwvYT4KICAgICAgICAgICAgICAgICAgICA8L2Rpdj4KICAgICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPSJyZWRlc19zb2NpYWlzIiBzdHlsZT0id2lkdGg6IDE1JTsgZGlzcGxheTogaW5saW5lLWJsb2NrOyBtYXJnaW46IDIlOyI+CiAgICAgICAgICAgICAgICAgICAgICAgIDxhIGhyZWY9Imh0dHBzOi8vd3d3LnlvdXR1YmUuY29tL3VzZXIvbG9qYXNiZW1vbCIgc3R5bGU9ImZvbnQtZmFtaWx5OiBzYW5zLXNlcmlmOyBmb250LXdlaWdodDogMjAwOyBmb250LXNpemU6IDY4JTsgY29sb3I6ICNmZmZmZmY7IHRleHQtZGVjb3JhdGlvbjogbm9uZTsgdGV4dC1hbGlnbjogY2VudGVyOyBmbG9hdDogY2VudGVyOyBkaXNwbGF5OiBibG9jazsiPgogICAgICAgICAgICAgICAgICAgICAgICAgICAgPGltZyBzcmM9Imh0dHBzOi8vZ2l0aHViLmNvbS9tYXRoZXVzaGVucmlxdWUtcHkvZW1haWxfcmVjb21lbmRhY2FvL3Jhdy9tYXN0ZXIvZGF0YS95b3V0dWJlLnBuZyIgLD0iIiBjbGFzcz0ibG9nb19zb2NpYWwiIHN0eWxlPSJtYXJnaW46IGF1dG87IHBhZGRpbmctYm90dG9tOiA1cHg7IGRpc3BsYXk6IGJsb2NrOyB3aWR0aDogOTAlOyBoZWlnaHQ6IGF1dG87IG9iamVjdC1maXQ6IHNjYWxlLWRvd247Ij4KICAgICAgICAgICAgICAgICAgICAgICAgICAgIHlvdXR1YmUKICAgICAgICAgICAgICAgICAgICAgICAgPC9hPgogICAgICAgICAgICAgICAgICAgIDwvZGl2PgogICAgICAgICAgICAgICAgPC9kaXY+CiAgICAgICAgICAgICAgICA8aDIgc3R5bGU9InRleHQtYWxpZ246IGNlbnRlcjsgZm9udC1zaXplOiAyNHB4OyBmb250LXdlaWdodDogYm9sZDsgY29sb3I6ICNmZmZmZmY7IG1hcmdpbjogMnB4OyI+Q2VudHJhbCBkZSBBdGVuZGltZW50byBCZW1vbDwvaDI+CiAgICAgICAgICAgICAgICA8aDIgc3R5bGU9InRleHQtYWxpZ246IGNlbnRlcjsgZm9udC1zaXplOiAyNHB4OyBmb250LXdlaWdodDogYm9sZDsgY29sb3I6ICNmZmZmZmY7IG1hcmdpbjogMnB4OyI+MDgwMCA3MjYgODMwMDwvaDI+CiAgICAgICAgICAgIDwvZGl2PgogICAgICAgIDwvZm9vdGVyPgogICAgPC9kaXY+CjwvYm9keT4KPC9odG1sPg=="

def email_campos(i):
    return {"nm_envio": "Recomendação de produtos", "nm_email": df_teste['Email'][i], "nm_subject": "Compre na Bemol sem sair de casa - Confira ofertas exclusivas",
           "nm_remetente": "Bemol Online", "email_remetente": "no-reply@bemol.com.br", "nm_reply": "no-reply@bemol.com.br",
           "dt_envio": str(date.today()), "hr_envio": datetime.now().strftime('%H:%M:%S'),
           "campos":"nome_produto1,nome_produto2,nome_produto3,preco_produto1,preco_produto2,preco_produto3,parcela_produto1,parcela_produto2,parcela_produto3,link_produto1,link_produto2,link_produto3,img_produto1,img_produto2,img_produto3",
           "valor": df_produtos['Nome'][df_teste['Recom1'][i]] + "," + df_produtos['Nome'][df_teste['Recom2'][i]] + "," + df_produtos['Nome'][df_teste['Recom3'][i]] + "," +
                    "{:.2f}".format(df_produtos['Preco'][df_teste['Recom1'][i]]) + "," + "{:.2f}".format(df_produtos['Preco'][df_teste['Recom2'][i]]) + "," + "{:.2f}".format(df_produtos['Preco'][df_teste['Recom3'][i]]) + "," +
                    "{:.2f}".format(df_produtos['Preco'][df_teste['Recom1'][i]]/4) + "," + "{:.2f}".format(df_produtos['Preco'][df_teste['Recom2'][i]]/4) + "," + "{:.2f}".format(df_produtos['Preco'][df_teste['Recom3'][i]]/4) + "," +
                    df_produtos['Link'][df_teste['Recom1'][i]] + "," + df_produtos['Link'][df_teste['Recom2'][i]] + "," + df_produtos['Link'][df_teste['Recom3'][i]] + "," +
                    df_produtos['Imagem'][df_teste['Recom1'][i]] + "," + df_produtos['Imagem'][df_teste['Recom2'][i]] + "," + df_produtos['Imagem'][df_teste['Recom3'][i]],
            }

corpo_emails = [email_campos(i) for i in range(len(df_teste['Email']))]

corpo_completo = { "emails": corpo_emails, "html": template }

response = requests.post(url, headers=headers, json=corpo_completo)
print(response.json())

''' Falta:
add o fuso horário
emojis!
como mudar o ponto para vírgula??????????
tratar exceções: quando não achar o produto pelo codigo, quando não houver imagem, etc...
dividir loop por 2mil clientes por envio
fazer o próprio encoding
'''

