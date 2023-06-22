from flask import Flask, render_template, request
import folium
import pandas as pd
import boto3
import awswrangler as wr
from IPython.display import HTML
import math
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
    fuso_horario_atual = pytz.timezone('Etc/GMT+3')
    data_inicio_objeto = datetime.fromisoformat(str(data_inicio))
    
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
    data_inicio_objeto = datetime.fromisoformat(str(data_inicio))
    
    # Aplica o fuso horário atual às datas
    inicio_fuso_horario = data_inicio_objeto.astimezone(fuso_horario_atual)
  
    
    # Formata as datas para o seu formato desejado
    formato_data = '%Y-%m-%d'
    dataformatada = inicio_fuso_horario.strftime(formato_data)
   
    
    # Retorna as datas formatadas
    return dataformatada


def adicionar_tres_horas(hora):
    # Converter a string de hora para um objeto datetime
    formato = "%Y-%m-%dT%H:%M:%SZ"
    hora_objeto = datetime.strptime(hora, formato)

    # Adicionar três horas ao objeto datetime
    nova_hora = hora_objeto + timedelta(hours=3)

    # Converter a nova hora de volta para o formato de string
    nova_hora_string = nova_hora.strftime(formato)

    return nova_hora_string

app = Flask(__name__)

# Configuração das credenciais da AWS
aws_access_key_id = 'AKIATJNIJV2QSZW6BUTB'
aws_secret_access_key = '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'
aws_region = 'us-east-1'

# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region)

# Executa a consulta no Athena
query = '''select * from ita_mix.trip where assetid in('1085761243725201408','1395639860612653056')'''

# Query retorna como um DataFrame
df = wr.athena.read_sql_query(
    query, database='ita_mix', s3_output='s3://ita-athena-queue/py/'
)

#df.dropna(subset=['latitude_trunc', 'longitude_trunc'], inplace=True)
#df['timestamp'] = df['timestamp'].apply(
    #lambda x: datetime.strptime(x, '%Y-%m-%dT%XZ'))
#df.drop_duplicates(['timestamp'], keep='first', inplace=True)
#df['heading'] = df['heading'].astype(int)


@app.route('/')
def index():
    # Obtém a lista de assetids únicos
    assetids = df['assetid'].unique().tolist()
    return render_template('index.html', assetids=assetids)

@app.route('/load_rangedata', methods=['POST'])
def load_rangedata():
        assetid = request.form['assetid']
        dff = df[df['assetid'] == assetid]
        inicio = dff['tripstart'].min()   
        datainicio = dff['tripstart'].unique().tolist()
        data_formatada2 = []  # Lista para armazenar as datas formatadas

        for data in datainicio:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada2.append(converter_fuso_horario_semsegundo(data))          
        fim = dff['tripend'].max()
        fim = converter_fuso_horario_semsegundo(fim)
        inicio = converter_fuso_horario_semsegundo(inicio)
        dt=obter_datas_entre(inicio,fim)
        datas = []
        for data in dt:
            for dts in data_formatada2:
                if data == dts:
                    datas.append(data)
        datas = list(set(datas))
        datas = sorted(datas)
        return render_template('index.html', assetids=df['assetid'].unique().tolist(), dts=datas, selected_assetid=assetid, assetid=assetid)

@app.route('/load_data', methods=['POST'])
def load_data():
        # Obtém o assetid selecionado no dropdown
        assetid = request.form['assetid']
        datass = request.form['data']
        # Filtra o DataFrame pelo assetid selecionado
        asset_trips = df[df['assetid'] == assetid]
        datainicio = asset_trips['tripstart'].unique().tolist()
        datafim = asset_trips['tripend'].unique().tolist()
        data_formatada = []  # Lista para armazenar as datas formatadas

        for data in datafim:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada.append(converter_fuso_horario(data))
        data_formatada2 = []  # Lista para armazenar as datas formatadas

        for data in datainicio:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada2.append(converter_fuso_horario(data))
        
        datas = list(zip(data_formatada2, data_formatada))
        datainicio_filtrada = []

        # Converter a variável 'datass' em um objeto datetime
        dataass_dt = datetime.strptime(datass, '%Y-%m-%d').date()

        # Percorrer a lista 'datainicio' e filtrar as datas correspondentes
        for data in data_formatada2:
            data_dt = datetime.strptime(data, '%Y-%m-%d %H:%M:%S').date()
            if data_dt == dataass_dt:
                datainicio_filtrada.append(data)
        datafim_filtrada = []

        for data_inicio in datainicio_filtrada:
            for data2, data in zip(data_formatada2, data_formatada):
                if data_inicio == data2:
                    datafim_filtrada.append(data)
     
        datas2 = list(zip(datainicio_filtrada, datafim_filtrada))
       
        # Converter as colunas para o tipo datetime
        #df1['tripstart'] = pd.to_datetime(df1['tripstart'])
        #df1['tripstart'] = datetime.fromisoformat(df1['tripstart'])
        # Encontrar a data mais antiga do tripstart e a data mais recente do tripend
        #datainicio = df1['tripstart'].min()
        #print(datainicio)
        return render_template('index.html', assetids=df['assetid'].unique().tolist(), datas= datas,datass= datas2, select_data=datass, selected_assetid=assetid)


@app.route('/load_trips', methods=['POST'])
def load_trips():
    # Obtém o assetid selecionado no dropdown
    assetid = request.form['assetid']
    
    # Filtra o DataFrame pelo assetid selecionado
    asset_trips = df[df['assetid'] == assetid]

    # Verifica se há dados disponíveis após a filtragem
    if not asset_trips.empty:
        # Obtém a lista de tripids para o asset selecionado
        tripids = asset_trips['tripid'].unique().tolist()
        
        return render_template('index.html', assetids=df['assetid'].unique().tolist(), tripids=tripids, selected_assetid=assetid)
    else:
        return render_template('error.html', message='No trips available for the selected asset')







@app.route('/map', methods=['POST'])
def show_map():
    entrada ="ok"
    # Obtém o assetid, tripid e a data selecionados no dropdown
    assetid = request.form['assetid']
    datainicial = request.form['data1']
    datafinal = request.form['data2']
    data_formatada1 = datetime.strptime(datainicial, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    data_formatada2 = datetime.strptime(datafinal, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    data_formatada1 = adicionar_tres_horas(data_formatada1)
    data_formatada2 = adicionar_tres_horas(data_formatada2)
    #tripid = request.form['tripid']
    dff = df[df['assetid'] == assetid]
    tripid = dff['tripid']

    
    query1 = f'''SELECT * FROM ita_mix.vw_latlong 
WHERE (timestamp >= '{data_formatada1}') AND (timestamp <= '{data_formatada2}')
and assetid like '{assetid}' '''
    # Query retorna como um DataFrame
    trip_data = wr.athena.read_sql_query(
        query1, database='ita_mix', s3_output='s3://ita-athena-queue/py/'
    )
   
    # Filtra o DataFrame pelo assetid e tripid selecionados
    #trip_data = df[(df['assetid'] == assetid) & (df['tripid'] == tripid) & df]
    # Verifica se há dados disponíveis após a filtragem
    if not trip_data.empty:
        # Cria o mapa

        lat = trip_data['latitude_trunc'].iloc[0]
        long = trip_data['longitude_trunc'].iloc[0]
        # Traça a rota da viagem selecionada
        coordinates = trip_data[['latitude_trunc',
                                 'longitude_trunc']].values.tolist()

    return render_template('index.html',cordinates = coordinates,  entrada=entrada, selected_assetid=assetid, assetid=assetid, data_formatada1=data_formatada1, data_formatada2=data_formatada2)
    #else:
        #return render_template('error.html', message='No data available for the selected trip')


if __name__ == '__main__':
    app.run(debug=True)
