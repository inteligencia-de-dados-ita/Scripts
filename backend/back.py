import pandas as pd
import boto3
import awswrangler as wr
import pytz
from datetime import datetime
from datetime import datetime, timedelta

def obter_datas_entre(data_inicial, data_final):
    datas = []
    data_atual = datetime.strptime(data_inicial, "%Y-%m-%d")
    data_fim = datetime.strptime(data_final, "%Y-%m-%d")

    while data_atual <= data_fim:
        datas.append(data_atual.strftime("%Y-%m-%d"))
        data_atual += timedelta(days=1)

    return datas


def converter_fuso_horario(data_inicio):
    # Define o fuso horário atual
    fuso_horario_atual = pytz.timezone('Etc/GMT+3')
    data_inicio_objeto = datetime.fromisoformat(str(data_inicio))
    
    # Aplica o fuso horário atual às datas
    inicio_fuso_horario = data_inicio_objeto.astimezone(fuso_horario_atual)
  
    
    # Formata as datas para o seu formato desejado
    formato_data = '%Y-%m-%d  %H:%M:%S'
    dataformatada = inicio_fuso_horario.strftime(formato_data)
   
    
    # Retorna as datas formatadas
    return dataformatada

# Configuração das credenciais da AWS
aws_access_key_id = 'AKIATJNIJV2QSZW6BUTB'
aws_secret_access_key = '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'
aws_region = 'us-east-1'

# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region)

# Executa a consulta no Athena
query = '''SELECT * FROM "ita_mix"."trip" where assetid like '1085761243725201408';'''

# Query retorna como um DataFrame
df = wr.athena.read_sql_query(
    query, database='ita_mix', s3_output='s3://ita-athena-queue/py/'
)


# Converter as colunas para o tipo datetime
df['tripend'] = pd.to_datetime(df['tripend'])
df['tripstart'] = pd.to_datetime(df['tripstart'])
datainicio = df['tripstart'].unique().tolist()
datafim = df['tripend'].unique().tolist()

# Encontrar a data mais antiga do tripstart e a data mais recente do tripend
#datainicio = df['tripstart'].min() 
datafim = df['tripend'].max()
data_formatada2 = []
#print(type(datainicio))
for data in datainicio:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada2.append(converter_fuso_horario(data))
#print(datainicio)
#datainicio = '2023-04-28 00:43:17+00:00'
datainicio_filtrada = []
datass = '2023-05-31'
dataass_dt = datetime.strptime(datass, '%Y-%m-%d').date()
fim = converter_fuso_horario(datafim)
#inicio = converter_fuso_horario(datainicio)
#print(inicio,fim)
#datas = obter_datas_entre(inicio, fim)
#print(datas)
# Percorrer a lista 'datainicio' e filtrar as datas correspondentes
for data in data_formatada2:
    data_dt = datetime.strptime(data, '%Y-%m-%d %H:%M:%S').date()
    if data_dt == dataass_dt:
        datainicio_filtrada.append(data)

print(datainicio_filtrada)
# Imprimir os resultados
#data_inicial= converter_fuso_horario(datainicio)
#data= converter_fuso_horario(datainicio)
#print(type(data))
#print("Data mais antiga do tripstart depois de formatar :", data)
#print("Data mais recente do tripend:", datafim)

#datas_entre = obter_datas_entre(data_inicial, data_final)
#print(datas_entre)
# Gerar as datas dentro do intervalo
#todas_as_datas = pd.date_range(start=datainicio, end=datafim).tolist()
# Suponha que você tenha uma lista chamada 'todas_as_datas' que contém todas as datas disponíveis

#datas_disponiveis = [data for data in todas_as_datas if datainicio <= data <= datafim]
#print(datas_disponiveis)
