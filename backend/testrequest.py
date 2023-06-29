import awswrangler as wr
import boto3
import datetime
from datetime import datetime
# Configuração das credenciais da AWS
aws_access_key_id = 'AKIATJNIJV2Q6XKQXK52'
aws_secret_access_key = '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'
aws_region = 'us-east-1'
boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region)
datainicio = '2023-05-25T09:19:40Z' 
datafim = '2023-05-25T10:03:42Z'
data1 = datetime.strptime(datainicio, '%Y-%m-%dT%H:%M:%SZ').date()
data2 = datetime.strptime(datainicio, '%Y-%m-%dT%H:%M:%SZ').date()
data1_formatted = datetime.strftime(data1, '%Y-%m-%dT%H:%M:%SZ')
data2_formatted = datetime.strftime(data2, '%Y-%m-%dT%H:%M:%SZ')
query1 = f'''SELECT * FROM ita_mix.lat_long 
WHERE "timestamp" >= '{data1_formatted}' AND "timestamp" <= '{data2_formatted}'
and assetid like '1085761243725201408' '''
# Query retorna como um DataFrame
try:
    trip_data = wr.athena.read_sql_query(
        query1, database='ita_mix'
    )
    print(trip_data['timestamp'])
except Exception as erro:
            print(erro)
