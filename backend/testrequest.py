import awswrangler as wr
import boto3
import datetime
from datetime import datetime, timedelta

def formatar_data():
    data_atual = datetime.utcnow()  # Pega a data e hora atual em UTC
    data_anterior = data_atual - timedelta(days=30)  # Subtrai 30 dias da data atual
    data_formatada = data_anterior.strftime("%Y-%m-%dT%H:%M:%SZ")  # Formata a data no formato especificado
    return data_formatada

# Exemplo de uso:
data_formatada = formatar_data()
print(data_formatada)
