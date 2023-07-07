from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import pytz
import json
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
    'carros por assinatura',
    'chaves identificação',
    'coming',
    'elcop',
    'comurg',
    'prefeitura de goiânia',
    'estoque ita',
    'express reforma',
    'hemolabor',
    'saneago',
    'csc',
    'manutenção',
    'cahves ita geral',
    'last mile',
    'itapar',
    'líder pets',
    'pdca engenharia',
    'triunfo concebra',
    'urban tecnologia',
    'Ultra Solar',
    'Tribunal de Contas',
    'Tencel Engenharia',
    'Teclog Fleet',
    'Suporte Sondagens Investigações',
    'Serviços - ADM',
    'SEPLAD/DF',
    'Rotagyn',
    'Redemob Consorcio',
    'Prospec Cell',
    'PRIMETEK',
    'Palme Vendas',
    'Outlet Primaveira',
    'Moriá Prestação de Serviços',
    'Líder Fomento Comercial',
    'IPEM - Instituto de Pesos e Medidas',
    'HSI Serviços e Comércio de Pneus',
    'HP Transportes Coletivos',
    'GOINFRA - Agência Goiana De Infr. E Transportes',
    'Goiás Rendering',
    'Goiás Minas|ITALAC',
    'Geogis Geotecnologia Ltda',
    'GAV - Pirenópolis Empr. Imob',
    'Franca e Pereira Ltda',
    'FAPEG - Fundação De Amparo Pesq. Est. Goiás',
    'Enebra',
    'Enapa Empresa Nac. Pavimentação',
    'DS Facility Ltda',
    'Dolp Engenharia',
    'DEC Agro Comercio e representações LTDA',
    'D A Tecnologia e Serviços',
    'Confrex Tec. Veicular',
    'Concebra',
    'Bold Ent',
    'Barão Especialidades',
    'Ambiente Consultoria',
    'Agromais Agropecuária'
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
                return df1
            except Exception as erro_nome:
                arquivo_erro = os.path.join(client_folder, f'{assetid}.json')
                erro = f"mensagem': '{erro_nome}'"
                json_data = json.dumps(erro)
                # Salvar a string JSON em um arquivo
                with open(arquivo_erro, 'w') as arquivo:
                    arquivo.write(json_data)
        
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
    try:
        
        df = execute_query(nome)
        print(nome +'.pkl')
        assetids = df['assetid'].unique().tolist()

        for assetid in assetids:
            try:
                execute_query1(assetid, datames)
                print(assetid +'.pkl')
            except Exception as erro_asset:
                # Obtém o nome do arquivo com base na variável 'assetid'
                nome_arquivo = f'{assetid}_erro.txt'
                print(nome_arquivo)
                # Cria o arquivo de texto com o nome especificado e escreve a mensagem de erro dentro dele
                with open(nome_arquivo, 'w') as arquivo:
                    arquivo.write(str(erro_asset))
    except Exception as erro_nome:
        # Obtém o nome do arquivo com base na variável 'nome'
        nome_arquivo = f'{nome}_erro.txt'
        print(nome_arquivo)
        # Cria o arquivo de texto com o nome especificado e escreve a mensagem de erro dentro dele
        with open(nome_arquivo, 'w') as arquivo:
            arquivo.write(str(erro_nome))
