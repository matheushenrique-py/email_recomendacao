from datetime import datetime
import pytz
import requests
import xml.etree.ElementTree as ET
import base64
import json
import argparse
import sys

import pandas as pd
import numpy as np

# headers para requisição para a API
url = "http://transacional-apiv2.allin.com.br/api/email/bulk"

'''até onde pude notar, o fuso-horário é tratado pela API.
O e-mail enviado no meu fuso horário (-3) é recebido no mesmo momento
lá em Manaus (-4).
Deixo o fuso apenas para deixar claro em que localização de referência 
'''
data_fuso = pytz.timezone('America/Recife').localize(datetime.now())


def ler_recomendacoes(arq):
    # Leitura da base de recomendações
    recom = pd.read_csv(arq, sep='\t')
    recom['e-mail'] = recom['e-mail'].apply(lambda x: x.replace(' ', ''))
    recom = recom[recom.Estoque > 5]

    # Organização da base de recomendações com as três colunas
    recomendacoes = []
    for pessoa in recom['CodCliente'].unique():
        recomendacoes.append(recom['Cod Recom'][recom['CodCliente'] == pessoa].values.tolist())

    for i in range(len(recomendacoes)):
        for j in range(2):
            try:
                recomendacoes[i, j]
            except:
                recomendacoes[i].append(np.NaN)
            recomendacoes[i] = recomendacoes[i][0:3]

    # formatação do DataFrame com as recomendações organizadas
    df = pd.DataFrame(data=recomendacoes, index=recom['CodCliente'].unique(), columns=['Recom1', 'Recom2', 'Recom3'])
    df.index.name = 'Cliente'
    df = df.merge(recom.loc[:, ['CodCliente', 'e-mail']].drop_duplicates(), left_on='Cliente', right_on='CodCliente')
    df.dropna(subset=['Recom3'], axis=0,  inplace=True)
    df.index = range(len(df))

    if is_teste:
        df = df.iloc[0:4, :]
        df['e-mail'] = ['matheusamorim@bemol.com.br', 'matheusamorim@bemol.com.br',
                        'matheushenrique.py@gmail.com', 'matheusamorim@bemol.com.br']
        #df['e-mail'] = ['sheilanobrega@bemol.com.br', 'lucasalmeida@bemol.com.br',
        #                'zulemavera@bemol.com.br', 'rafaelasousa@bemol.com.br', 'matheusamorim@bemol.com.br']

    return df


def ler_db_produtos(site):
    # request para acessar lista de produtos
    if site == 'bol':
        url_xml = "https://www.bemol.com.br/feeds/google-merchant"
    else:
        url_xml = "https://www.bemolfarma.com.br/feeds/google-merchant-farma"
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
    df = pd.DataFrame({'Nome': produtos, 'Imagem': images, 'Link': links, 'Preco': precos}, index=codigos)
    df['Nome'] = df['Nome'].apply(lambda x: x.replace(',', '.'))

    return df


def linkar(df_recom, df_prod):
    a, b, c, d, e = ([] for i in range(5))

    for i in range(len(df_recom)):
        try:
            if df_prod['Imagem'][df_recom['Recom1'][i]].find('sem-foto.gif') > 0:
                a.append(-2)
            else:
                a.append(df_prod['Preco'][df_recom['Recom1'][i]])
        except:
            a.append(-1)
        try:
            if df_prod['Imagem'][df_recom['Recom2'][i]].find('sem-foto.gif') > 0:
                b.append(-2)
            else:
                b.append(df_prod['Preco'][df_recom['Recom2'][i]])
        except:
            b.append(-1)
        try:
            if df_prod['Imagem'][df_recom['Recom3'][i]].find('sem-foto.gif') > 0:
                c.append(-2)
            else:
                c.append(df_prod['Preco'][df_recom['Recom3'][i]])
        except:
            c.append(-1)
        try:
            if df_prod['Imagem'][df_recom['Recom4'][i]].find('sem-foto.gif') > 0:
                d.append(-2)
            else:
                d.append(df_prod['Preco'][df_recom['Recom4'][i]])
        except:
            d.append(-1)
        try:
            if df_prod['Imagem'][df_recom['Recom5'][i]].find('sem-foto.gif') > 0:
                e.append(-2)
            else:
                e.append(df_prod['Preco'][df_recom['Recom5'][i]])
        except:
            e.append(-1)

        df_link = pd.DataFrame({'Recom1': a, 'Recom2': b, 'Recom3': c, 'Recom4': d, 'Recom5': e})

        excluidos = []
        for i in range(len(df_link)):
            count = 0
            for j in range(len(df_link.columns)):
                if df_link.iloc[i, j] > 5:
                    count += 1
            if count < 3:
                excluidos.append(i)

    return excluidos

# função da formatação do json dos emails
def email_campos(i):
    return {"nm_envio": nome, "nm_email": df_recom['e-mail'][i], "nm_subject": teste_assunto + assunto,
           "nm_remetente": "Bemol Online", "email_remetente": "no-reply@bemol.com.br", "nm_reply": "no-reply@bemol.com.br",
           "dt_envio": data_fuso.strftime('%Y-%m-%d'), "hr_envio": data_fuso.now().strftime('%H:%M:%S'),
           "campos":"nome_produto1,nome_produto2,nome_produto3,preco_produto1,preco_produto2,preco_produto3,parcela_produto1,parcela_produto2,parcela_produto3,link_produto1,link_produto2,link_produto3,img_produto1,img_produto2,img_produto3",
           "valor": df_prod['Nome'][df_recom['Recom1'][i]] + "," + df_prod['Nome'][df_recom['Recom2'][i]] + "," + df_prod['Nome'][df_recom['Recom3'][i]] + "," +
                    "{:.2f}".format(df_prod['Preco'][df_recom['Recom1'][i]]) + "," + "{:.2f}".format(df_prod['Preco'][df_recom['Recom2'][i]]) + "," + "{:.2f}".format(df_prod['Preco'][df_recom['Recom3'][i]]) + "," +
                    "{:.2f}".format(df_prod['Preco'][df_recom['Recom1'][i]]/4) + "," + "{:.2f}".format(df_prod['Preco'][df_recom['Recom2'][i]]/4) + "," + "{:.2f}".format(df_prod['Preco'][df_recom['Recom3'][i]]/4) + "," +
                    df_prod['Link'][df_recom['Recom1'][i]] + "," + df_prod['Link'][df_recom['Recom2'][i]] + "," + df_prod['Link'][df_recom['Recom3'][i]] + "," +
                    df_prod['Imagem'][df_recom['Recom1'][i]] + "," + df_prod['Imagem'][df_recom['Recom2'][i]] + "," + df_prod['Imagem'][df_recom['Recom3'][i]],
            }

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script de envio em lotes de e-mail marketing personalizado')
    parser.add_argument('arq_csv', help='Nome do arquivo .csv com as regras de recomendação')
    parser.add_argument('--site', '-s', help='Qual a site: "bol" ou "farma"', required=True)
    parser.add_argument('--oficial', '-o',
                        help='Roda o script no modo de de envio oficial. Por padrão roda em modo teste',
                        action='store_true')
    parser.add_argument('--lote', '-l', help='Tamanho dos lotes de envio de e-mal', default=1000, type=int)

    args = parser.parse_args()

    site = args.site
    if site not in ('bol', 'farma'):
        print('Site inválido. Por favor insira qual a site do envio: "bol" ou "farma"')
        sys.exit(status=1)
    if args.oficial:
        is_teste = False
        teste_assunto = ''
    else:
        is_teste = True
        teste_assunto = 'Teste - '

    nome = input('Insira o nome do envio: ')
    assunto = input('Insira o assunto do e-mail: ')

    # carrega o devido template
    if site == 'bol':
        with open('data/templates/template_css_inliner.html', 'rb') as file:
            template = base64.standard_b64encode(file.read()).decode('utf-8')
    else:
        with open('data/templates/template_farma_css_inliner.html', 'rb') as file:
            template = base64.standard_b64encode(file.read()).decode('utf-8')

    # carrega o cabeçalho para a requisão à API
    with open('data/bases/header_api.txt', 'r', encoding='utf-8') as file:
        headers = json.load(file)

    df_recom = ler_recomendacoes(args.arq_csv)
    df_prod = ler_db_produtos(args.site)
    df_recom.drop(linkar(df_recom, df_prod), axis='index', inplace=True)
    df_recom.index = range(len(df_recom))

    # envio dos emails dividindo por lotes
    LEN_REQ = args.lote
    k = 0
    if len(df_recom['e-mail']) > LEN_REQ:
        while len(df_recom['e-mail'][k*LEN_REQ:]) > LEN_REQ:
            envio(k*LEN_REQ, (k+1)*LEN_REQ)
            k+=1
        envio(k*LEN_REQ, len(df_recom['e-mail']))
    else:
        envio(0, len(df_recom['e-mail']))

# TODO: tratar exceções: quando não achar o produto pelo codigo, quando não houver imagem, etc...
# TODO: emojis!
# TODO: como mudar o ponto para vírgula??????????