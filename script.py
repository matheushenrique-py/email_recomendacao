from datetime import datetime
import pytz
import requests
import xml.etree.ElementTree as ET
import base64
import json

import pandas as pd
import numpy as np

'''até onde pude notar, o fuso-horário é tratado pela API.
O e-mail enviado no meu fuso horário (-3) é recebido no mesmo momento
lá em Manaus (-4).
Deixo o fuso apenas para deixar claro em que localização eu programei 
'''

loja = input('Rodar para bol ou farma: ')
is_teste = True

data_fuso = pytz.timezone('America/Recife').localize(datetime.now())


def ler_recomendacoes():
    # Leitura da base de recomendações
    if loja == 'bol':
        recom = pd.read_csv('data/bases/base_final_31-03.csv', sep='\t')
    else:
        recom = pd.read_csv('data/bases/base_farma_teste.csv', sep='\t')

    recom['e-mail'] = recom['e-mail'].apply(lambda x: x.replace(' ', ''))
    recom = recom[recom.Estoque > 5]

    # Organização da base de recomendações com as três colunas
    recomendacoes = []
    for pessoa in recom['CodCliente'].unique():
        recomendacoes.append(recom['Cod Recom'][recom['CodCliente'] == pessoa].values.tolist())

    for i in range(len(recomendacoes)):
        for j in range(2):
            try:
                recomendacoes[i,j]
            except:
                recomendacoes[i].append(np.NaN)
            recomendacoes[i] = recomendacoes[i][0:3]

    df = pd.DataFrame(data=recomendacoes, index=recom['CodCliente'].unique(), columns=['Recom1', 'Recom2', 'Recom3'])
    df.index.name = 'Cliente'
    df = df.merge(recom.loc[:,['CodCliente','e-mail']].drop_duplicates(), left_on='Cliente', right_on='CodCliente')

    df.dropna(subset=['Recom3'], axis=0,  inplace=True)
    df.index = range(len(df))

    if is_teste:
        df = df.iloc[0:3, :]
        df['e-mail'] = ['matheusamorim@bemol.com.br', 'matheusamorim@bemol.com.br', 'matheushenrique.py@gmail.com']
        #df['e-mail'] = ['sheilanobrega@bemol.com.br', 'lucasalmeida@bemol.com.br', 'zulemavera@bemol.com.br', 'rafaelasousa@bemol.com.br', 'matheusamorim@bemol.com.br']

    return df


def ler_db_produtos():
    # request para acessar lista de produtos
    if loja == 'bol':
        url_xml = "https://www.bemol.com.br/feeds/google-merchant"
    elif loja == 'farma':
        url_xml = "https://www.bemolfarma.com.br/feeds/google-merchant-farma"
    else:
        print('Erro. Tratar isso melhor')

    header = {"Accept": "application/xml"}
    r = requests.get(url_xml, headers=header)

    # Gera um DataFrame com os produtos da BOL
    tree = ET.ElementTree(ET.fromstring(r.content))
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
    df_produtos['Nome'] = df_produtos['Nome'].apply(lambda x: x.replace(',', '.'))

    return df_produtos

# função da formatação do json dos emails
def email_campos(i):
    try:
        return {"nm_envio": "Recomendação de produtos", "nm_email": df['e-mail'][i], "nm_subject": "Teste - Compre na Bemol sem sair de casa - Confira ofertas exclusivas!",
               "nm_remetente": "Bemol Online", "email_remetente": "no-reply@bemol.com.br", "nm_reply": "no-reply@bemol.com.br",
               "dt_envio": data_fuso.strftime('%Y-%m-%d'), "hr_envio": data_fuso.now().strftime('%H:%M:%S'),
               "campos":"nome_produto1,nome_produto2,nome_produto3,preco_produto1,preco_produto2,preco_produto3,parcela_produto1,parcela_produto2,parcela_produto3,link_produto1,link_produto2,link_produto3,img_produto1,img_produto2,img_produto3",
               "valor": df_produtos['Nome'][df['Recom1'][i]] + "," + df_produtos['Nome'][df['Recom2'][i]] + "," + df_produtos['Nome'][df['Recom3'][i]] + "," +
                        "{:.2f}".format(df_produtos['Preco'][df['Recom1'][i]]) + "," + "{:.2f}".format(df_produtos['Preco'][df['Recom2'][i]]) + "," + "{:.2f}".format(df_produtos['Preco'][df['Recom3'][i]]) + "," +
                        "{:.2f}".format(df_produtos['Preco'][df['Recom1'][i]]/4) + "," + "{:.2f}".format(df_produtos['Preco'][df['Recom2'][i]]/4) + "," + "{:.2f}".format(df_produtos['Preco'][df['Recom3'][i]]/4) + "," +
                        df_produtos['Link'][df['Recom1'][i]] + "," + df_produtos['Link'][df['Recom2'][i]] + "," + df_produtos['Link'][df['Recom3'][i]] + "," +
                        df_produtos['Imagem'][df['Recom1'][i]] + "," + df_produtos['Imagem'][df['Recom2'][i]] + "," + df_produtos['Imagem'][df['Recom3'][i]],
                }
    except KeyError: # caso não tenha o código do produto na BOL...
        pass

# função de envio do lote de emails
def envio(inicio, fim):
    if is_teste:
        file = open('data/logs/log_teste' + str(data_fuso.strftime('%Y-%m-%d')), 'a')
    else:
        file = open('data/logs/log_' + str(data_fuso.strftime('%Y-%m-%d')), 'a')
    file.write('*** envio dia ' + str(data_fuso.strftime('%Y-%m-%d')) + ' envio nº ' + str(int(inicio/LEN_REQ)) + '***\n')

    corpo_emails = []
    for i in range(inicio, fim):
        if email_campos(i) is not None:
            corpo_emails.append(email_campos(i))
            file.write(email_campos(i)['nm_email'] + ' - '+ email_campos(i)['valor']+ '\n\n')
        else:
            print('produto não encontrado')

    corpo_completo = {"emails": corpo_emails, "html": template}
    response = requests.post(url, headers=headers, json=corpo_completo)
    file.write('\n\n')
    file.close()

    return print(response.json())

# headers para requisição para a API
url = "http://transacional-apiv2.allin.com.br/api/email/bulk"

with open('data/bases/header_api.txt', 'r', encoding='utf-8') as file:
    headers = json.load(file)

df = ler_recomendacoes()

if loja == 'bol':
    with open('data/templates/template_css_inliner.html', 'rb') as file:
        template = base64.standard_b64encode(file.read()).decode('utf-8')
elif loja == 'farma':
    with open('data/templates/template_farma_css_inliner.html', 'rb') as file:
        template = base64.standard_b64encode(file.read()).decode('utf-8')
else:
    print('Erro. Tratar isso melhor')

df_produtos = ler_db_produtos()

# envio dos emails dividindo por lotes
LEN_REQ = 2
k = 0
if len(df['e-mail']) > LEN_REQ:
    while len(df['e-mail'][k*LEN_REQ:]) > LEN_REQ:
        envio(k*LEN_REQ, (k+1)*LEN_REQ)
        k+=1
    envio(k*LEN_REQ, len(df['e-mail']))
else:
    envio(0, len(df['e-mail']))

''' Falta:
tratar exceções: quando não achar o produto pelo codigo, quando não houver imagem, etc...

emojis!
como mudar o ponto para vírgula??????????
'''