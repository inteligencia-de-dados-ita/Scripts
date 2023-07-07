from flask import Flask, render_template, request
from flask_caching import Cache
import folium
import pandas as pd
import boto3
import awswrangler as wr
from IPython.display import HTML
import math
import json
import pytz
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
# Obtém o valor da variável de ambiente API_KEY
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
aws_region = os.getenv('aws_region')
# Configuração das credenciais da AWS
aws_region = 'us-east-1'
# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id= aws_access_key_id,
                            aws_secret_access_key= aws_secret_access_key,
                            region_name=aws_region)
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
def execute_query2(assetids , datainicio, datafim, cliente):
    cache_folder = 'temp'
    client_folder = os.path.join(cache_folder, 'cacheprincipal')
    
    # Verificar se a pasta temp existe, se não, criar
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
    
    # Verificar se a pasta do cliente existe, se não, criar
    if not os.path.exists(client_folder):
        os.makedirs(client_folder)
    
    # Verificar se o arquivo de cache existe para o cliente e nome especificados
    cache_file = os.path.join(client_folder, f'{cliente}_por_data.pkl')
    if os.path.exists(cache_file):
        # Carregar dados do cache a partir do arquivo
        df2 = pd.read_pickle(cache_file)
        return df2
    else:
        veiculos_formatados = ', '.join(["'{}'".format(v) for v in assetids])  # Formata cada veículo com aspas
        where_clause = "WHERE assetid IN ({})".format(veiculos_formatados)
        query2 = f'''SELECT *
        FROM ita_mix.trip
        {where_clause}
        AND (tripstart >= ('{datainicio}'))
        AND (tripstart <= ('{datafim}')) 
        and tripstart is not null
        and tripend is not null 
        ORDER BY tripstart;'''
        print(query2)
        df2 = wr.athena.read_sql_query(
        query2,database='ita_mix', s3_output='s3://ita-athena-queue/py/',athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
         # Salvar os dados do cache no arquivo
        df2.to_pickle(cache_file)   
        return df2
dia = '2023-07-06'
nome='enel'
assetids = ['-6716286443367253036','-715432496393638048','-632409115753110773']
formato = "%Y-%m-%dT%H:%M:%SZ"
data = datetime.strptime(dia, '%Y-%m-%d')
datatz = data.strftime('%Y-%m-%d %H:%M:%S')
# Adicionando um dia
data_nova = data + timedelta(days=1)
datatz2 = data_nova.strftime('%Y-%m-%d %H:%M:%S')
df2 =  execute_query2(assetids , datatz, datatz2, nome)
print(df2)
