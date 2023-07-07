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
assetid = '1060337872398565376'
cache_folder = 'temp'
client_folder = os.path.join(cache_folder, 'cacheprincipal')

# Verificar se a pasta temp existe, se não, criar
if not os.path.exists(cache_folder):
    os.makedirs(cache_folder)

# Verificar se a pasta do cliente existe, se não, criar
if not os.path.exists(client_folder):
    os.makedirs(client_folder)

# Verificar se o arquivo de cache existe para o cliente e nome especificados
cache_file = os.path.join(client_folder, f'{assetid}.pkl')
if os.path.exists(cache_file):
    # Carregar dados do cache a partir do arquivo
    df1 = pd.read_pickle(cache_file)
    fim = df1['tripstart'].max()
    # Obtém a data e hora atual em UTC
    agora = datetime.now(pytz.utc)
    fuso_horario_brasilia = pytz.timezone('Etc/GMT')
    # Converte a data e hora para o fuso horário de Brasília
    agora_brasilia = agora.astimezone(fuso_horario_brasilia)
    # Formata a data e hora no estilo desejado
    formato = "%Y-%m-%dT%H:%M:%SZ"

    final = datetime.fromisoformat(fim)
    datafinal = final.strftime(formato)
    data_formatada = agora_brasilia.strftime(formato)
    print(datafinal)
    print(data_formatada)
    if datafinal < data_formatada:
        try:
            query1 = f'''SELECT *
            FROM ita_mix.trip
            WHERE assetid IN ('{assetid}')
            AND (tripstart >= ('{datafinal}')) 
            and tripstart is not null
            and tripend is not null 
            ORDER BY tripstart;'''
            df1 = wr.athena.read_sql_query(
            query1,database='ita_mix', s3_output='s3://ita-athena-queue/py/',athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
            # Salvar os dados do cache no arquivo
            df1.to_pickle(cache_file)
            print(df1['tripstart'])
            
        except Exception as erro_nome:
            arquivo_erro = os.path.join(client_folder, f'{assetid}.json')
            erro = f"mensagem': '{erro_nome}'"
            json_data = json.dumps(erro)
            # Salvar a string JSON em um arquivo
            with open(arquivo_erro, 'w') as arquivo:
                arquivo.write(json_data)
    else:
        print("atual")