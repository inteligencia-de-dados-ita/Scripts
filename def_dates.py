import pandas as pd
import pytz
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
    fuso_horario_atual = pytz.timezone('Etc/GMT+6')
    # Remove o 'T' e o 'Z' da string de entrada
    data_inicio_sem_tz = data_inicio.replace('T', ' ').replace('Z', '')

    # Converte a string para um objeto datetime
    data_inicio_objeto = datetime.fromisoformat(data_inicio_sem_tz)

    # Aplica o fuso horário atual às datas
    inicio_fuso_horario = data_inicio_objeto.astimezone(fuso_horario_atual)

    # Formata as datas para o seu formato desejado
    formato_data = '%Y-%m-%d %H:%M:%S'
    dataformatada = inicio_fuso_horario.strftime(formato_data)

    # Retorna as datas formatadas
    return dataformatada
def converter_fuso_horario3(data_inicio):
    # Define o fuso horário atual
    fuso_horario_atual = pytz.timezone('Etc/GMT+3')
    # Remove o 'T' e o 'Z' da string de entrada
    data_inicio_sem_tz = data_inicio.replace('T', ' ').replace('Z', '')

    # Converte a string para um objeto datetime
    data_inicio_objeto = datetime.fromisoformat(data_inicio_sem_tz)

    # Aplica o fuso horário atual às datas
    inicio_fuso_horario = data_inicio_objeto.astimezone(fuso_horario_atual)

    # Formata as datas para o seu formato desejado
    formato_data = '%Y-%m-%d %H:%M:%S'
    dataformatada = inicio_fuso_horario.strftime(formato_data)

    # Retorna as datas formatadas
    return dataformatada

def converter_fuso_horario_semsegundo(data_inicio):
    # Define o fuso horário atual
    fuso_horario_atual = pytz.timezone('Etc/GMT+3')

    # Remove o 'T' e o 'Z' da string de entrada
    data_inicio_semsegundo = data_inicio.replace('T', ' ').replace('Z', '')

    # Converte a string para um objeto datetime
    data_inicio_objeto = datetime.strptime(data_inicio_semsegundo, "%Y-%m-%d %H:%M:%S")

    # Aplica o fuso horário atual às datas
    inicio_fuso_horario = data_inicio_objeto.astimezone(fuso_horario_atual)

    # Formata as datas para o seu formato desejado
    formato_data = '%Y-%m-%d'
    dataformatada = inicio_fuso_horario.strftime(formato_data)

    # Retorna as datas formatadas
    return dataformatada
def formatar_data():
    data_atual = datetime.utcnow()  # Pega a data e hora atual em UTC
    data_anterior = data_atual - timedelta(days=30)  # Subtrai 30 dias da data atual
    data_formatada = data_anterior.strftime("%Y-%m-%dT%H:%M:%SZ")  # Formata a data no formato especificado
    return data_formatada

def adicionar_tres_horas(hora):
    # Converter a string de hora para um objeto datetime
    formato = "%Y-%m-%d %H:%M:%S"
    hora_objeto = datetime.strptime(hora, formato)

    # Adicionar três horas ao objeto datetime
    nova_hora = hora_objeto + timedelta(hours=3)

    # Converter a nova hora de volta para o formato de string
    nova_hora_string = nova_hora.strftime(formato)

    return nova_hora_string
def obterdts(dfglobal):
        dts = []
        if not dfglobal.empty:            
            inicio = dfglobal['tripstart'].min()   
            fim = dfglobal['tripend'].max()
            datainicio = dfglobal['tripstart'].unique().tolist()
            data_formatada2 = []  # Lista para armazenar as datas formatadas
            dts = []
            for data in datainicio:
                # Aplica a função de conversão de fuso horário para cada data
                data_formatada2.append(converter_fuso_horario_semsegundo(data))          
            
            fim = converter_fuso_horario_semsegundo(fim)
            inicio = converter_fuso_horario_semsegundo(inicio)
            dt=obter_datas_entre(inicio,fim)
            
            for data in dt:
                for datas in data_formatada2:
                    if data == datas:
                        dts.append(data)
            
            dts = list(set(dts))
            dts = sorted(dts)
            return dts
        else:
            return dts