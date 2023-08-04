import pandas as pd
import boto3
import awswrangler as wr
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from flask import render_template
import def_dates
import app
# Obtém o valor da variável de ambiente API_KEY
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
aws_region = os.getenv('aws_region')


# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id= aws_access_key_id,
                            aws_secret_access_key= aws_secret_access_key,
                            region_name=aws_region)

# Executa a consulta no Athena
#@cache.memoize(timeout=900)  # Defina o tempo de expiração do cache em segundos
# def execute_query1(assetid, datames, nome):
#     cache_folder = 'temp'
#     client_folder = os.path.join(cache_folder, 'cacheprincipal')
    
#     # Verificar se a pasta temp existe, se não, criar
#     if not os.path.exists(cache_folder):
#         os.makedirs(cache_folder)
    
#     # Verificar se a pasta do cliente existe, se não, criar
#     if not os.path.exists(client_folder):
#         os.makedirs(client_folder)
    
#     # Verificar se o arquivo de cache existe para o cliente e nome especificados
#     cache_file = os.path.join(client_folder, f'{assetid}.pkl')
#     if os.path.exists(cache_file):
#         # Carregar dados do cache a partir do arquivo
#         df1 = pd.read_pickle(cache_file)
#         return df1
#     else:
#         query1 = f'''SELECT *
#         FROM {nome}.trip
#         WHERE assetid IN ('{assetid}')
#         AND (tripstart >= ('{datames}')) 
#         and tripstart is not null
#         and tripend is not null 
#         ORDER BY tripstart;'''
#         print(query1)
#         df1 = wr.athena.read_sql_query(
#         query1,database=f'{nome}', s3_output='s3://ita-athena-queue/py/',athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
#          # Salvar os dados do cache no arquivo
#         df1.to_pickle(cache_file)   
#         return df1
    
#@cache.memoize(timeout=900)  # Defina o tempo de expiração do cache em segundos
##consulta lista assets por garagem
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
    hoje = datetime.today()
    hoje = hoje.strftime("%Y-%m-%d")
    cache_file = os.path.join(client_folder, f'assets{nome}_{hoje}.pkl')
    print(cache_file)
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

## exibe a viagem com as placas
def execute_query2(assetids , datainicio, datafim, cliente, nome):
    condicao = False
    hoje = datetime.today()
    hoje = hoje.strftime("%Y-%m-%d")
    cache_folder = 'temp'
    client_folder = os.path.join(cache_folder, 'cacheprincipal')
    
    # Verificar se a pasta temp existe, se não, criar
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
    
    # Verificar se a pasta do cliente existe, se não, criar
    if not os.path.exists(client_folder):
        os.makedirs(client_folder)
    if datainicio[:10]==hoje:
        hora = datetime.today()
        hora = hora.strftime("%Y-%m-%d %H")
        cache_file = os.path.join(client_folder, f'trips{cliente}_{hora}.pkl')
    # Verificar se o arquivo de cache existe para o cliente e nome especificados
    else:
        cache_file = os.path.join(client_folder, f'trips{cliente}_{datainicio[:10]}.pkl')
    print(cache_file)
    if os.path.exists(cache_file) and datainicio[:10]== hoje:
        horaatual = datetime.today()
        horaatual = horaatual.strftime("%H")
        print("cache funcionou")
        print(horaatual)
        print(cache_file[(len(cache_file)-6):(len(cache_file)-4)])
        if(horaatual==cache_file[(len(cache_file)-6):(len(cache_file)-4)]):
            print("hora igual")
            df2 = pd.read_pickle(cache_file)
            return df2  
            
        
    if os.path.exists(cache_file) and datainicio[:10]!= hoje: 
        # Carregar dados do cache a partir do arquivo
        df2 = pd.read_pickle(cache_file)
        return df2     
    else:
        datainicio = def_dates.adicionar_tres_horas(datainicio)
        datafim = def_dates.adicionar_tres_horas(datafim)
        veiculos_formatados = ', '.join(["'{}'".format(v) for v in assetids])  # Formata cada veículo com aspas
        where_clause = "WHERE trip.assetid IN ({})".format(veiculos_formatados)
        query2 = f'''SELECT 
        trip.assetid,
        asset.description,
        date_parse(trip.tripstart, '%Y-%m-%dT%H:%i:%sZ') AS tripstart,
        date_parse(trip.tripend, '%Y-%m-%dT%H:%i:%sZ') AS tripend,
        asset.assetimageurl
    FROM 
        {nome}.trip as trip
    JOIN 
        "ita_mix"."asset_to_garage" as asset on CAST(asset.assetid AS VARCHAR) = trip.assetid
        {where_clause}
       AND (date_parse(trip.tripstart, '%Y-%m-%dT%H:%i:%sZ') >= TIMESTAMP '{datainicio}')
    AND (date_parse(trip.tripstart, '%Y-%m-%dT%H:%i:%sZ') <= TIMESTAMP '{datafim}')
    AND trip.tripstart IS NOT NULL
    AND trip.tripend IS NOT NULL 
ORDER BY 
    trip.tripstart;'''
        print(query2)
        df2 = wr.athena.read_sql_query(
        query2,database=f'{nome}', s3_output='s3://ita-athena-queue/',athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
         # Salvar os dados do cache no arquivo
        
        print(df2.head())
        if df2.empty:
            print("entrou no vazio")
            return df2
        df2.to_pickle(cache_file) 
        return df2
