from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime, timedelta
# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

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
nomes = [
    'car sharing'
    
]

def formatar_data():
    data_atual = datetime.utcnow()  # Pega a data e hora atual em UTC
    data_anterior = data_atual - timedelta(days=30)  # Subtrai 30 dias da data atual
    data_formatada = data_anterior.strftime("%Y-%m-%dT%H:%M:%SZ")  # Formata a data no formato especificado
    return data_formatada
def execute_query1(assetid, datames):
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
        return df1
    else:
        query1 = f'''SELECT *
        FROM ita_mix.trip
        WHERE assetid IN ('{assetid}')
        AND (tripstart >= ('{datames}')) 
        and tripstart is not null
        and tripend is not null 
        ORDER BY tripstart;'''
        df1 = wr.athena.read_sql_query(
        query1,database='ita_mix', s3_output='s3://ita-athena-queue/py/',athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
         # Salvar os dados do cache no arquivo
        df1.to_pickle(cache_file)   
        return df1

def execute_query(nome): 
    cache_folder = 'temp'
    client_folder = os.path.join(cache_folder, 'cacheprincipal')
    
    # Verificar se a pasta temp existe, se não, criar
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
    
    # Verificar se a pasta do cliente existe, se não, criar
    if not os.path.exists(client_folder):
        os.makedirs(client_folder)
    
    # Verificar se o arquivo de cache existe para o cliente e nome especificados
    cache_file = os.path.join(client_folder, f'{nome}.pkl')
    if os.path.exists(cache_file):
        # Carregar dados do cache a partir do arquivo
        df = pd.read_pickle(cache_file)
        
    else:
        # Executar a consulta e salvar os resultados no arquivo de cache
        query = f'''SELECT CAST(asset.assetid AS varchar)as assetid,
        asset.siteid,
        asset.assetimageurl,
        asset.description,
        site.sitename
        FROM "ita_mix"."asset_to_garage"  as asset
        join "ita_mix"."sitegroup"  as site on asset.siteid = site.siteid 
        WHERE asset.userstate like 'Available'
        and (LOWER(mae_0) LIKE LOWER('%{nome}%')
        OR LOWER(mae_1) LIKE LOWER('%{nome}%')
        OR LOWER(mae_2) LIKE LOWER('%{nome}%')
        OR LOWER(mae_3) LIKE LOWER('%{nome}%')
        OR LOWER(mae_4) LIKE LOWER('%{nome}%')
        OR LOWER(mae_5) LIKE LOWER('%{nome}%')
        OR LOWER(mae_6) LIKE LOWER('%{nome}%'))'''

        # Query retorna como um DataFrame
        df = wr.athena.read_sql_query(
            query, database='ita_mix', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
        
        # Salvar os dados do cache no arquivo
        df.to_pickle(cache_file)
    
    return df
datames= formatar_data()
for nome in nomes:
    df =execute_query(nome)
    assetids = df['assetid'].unique().tolist()
    for assetid in assetids:
        execute_query1(assetid, datames)