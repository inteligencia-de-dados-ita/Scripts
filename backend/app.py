from flask import Flask, render_template, request
import folium
import pandas as pd
import boto3
import awswrangler as wr
from IPython.display import HTML
import math
import pytz
from datetime import datetime, timedelta
#from dotenv import load_dotenv
import os
# Obtém o valor da variável de ambiente API_KEY
#aws_access_key_id = os.getenv('aws_access_key_id')
#aws_secret_access_key = os.getenv('aws_secret_access_key')
#aws_region = os.getenv('aws_region')
# Carrega as variáveis de ambiente do arquivo .env
#load_dotenv()
# Configuração das credenciais da AWS
aws_access_key_id = 'AKIATJNIJV2Q6XKQXK52'
aws_secret_access_key = '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'
aws_region = 'us-east-1'
dts = []
dfglobal = None
nomes =[]
df = None
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




# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id= aws_access_key_id,
                            aws_secret_access_key= aws_secret_access_key,
                            region_name=aws_region)
name = 'enel'
# Executa a consulta no Athena



#df.dropna(subset=['latitude_trunc', 'longitude_trunc'], inplace=True)
#df['timestamp'] = df['timestamp'].apply(
    #lambda x: datetime.strptime(x, '%Y-%m-%dT%XZ'))
#df.drop_duplicates(['timestamp'], keep='first', inplace=True)
#df['heading'] = df['heading'].astype(int)


@app.route('/')
def index():
    global nomes
 
    nomes = [
    'enel',
    'jaime câmara',
    'aqualis',
    'car sharing',
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

    return render_template('index.html', nomes=nomes)


@app.route('/select_asset', methods=['POST'])
def select_asset(): 
    global nomes
    global df
    nome = request.form['client']
    query = f'''SELECT asset.assetid,
    asset.siteid,
    asset.assetimageurl,
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
    query, database='ita_mix')
    
    # Obtém a lista de assetids únicos
    assetids = df['assetid'].unique().tolist()
    return render_template('index.html', assetids=assetids, selected_nome=nome, nomes=nomes)
@app.route('/load_rangedata', methods=['POST'])
def load_rangedata():
        global nomes
        global df
        global dfglobal
        global dts
        dts = []
        assetid = request.form['assetid']
        query1 = f'''select * from ita_mix.trip where assetid in('{assetid}')'''
        df1 = wr.athena.read_sql_query(
        query1, database='ita_mix')
        dfglobal = df1[df1['assetid'] == assetid]
        
        inicio = dfglobal['tripstart'].min()   
        datainicio = dfglobal['tripstart'].unique().tolist()
        data_formatada2 = []  # Lista para armazenar as datas formatadas

        for data in datainicio:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada2.append(converter_fuso_horario_semsegundo(data))          
        fim = dfglobal['tripend'].max()
        fim = converter_fuso_horario_semsegundo(fim)
        inicio = converter_fuso_horario_semsegundo(inicio)
        dt=obter_datas_entre(inicio,fim)
        
        for data in dt:
            for datas in data_formatada2:
                if data == datas:
                    dts.append(data)
        
        dts = list(set(dts))
        dts = sorted(dts)
        return render_template('index.html', fim = fim, inicio = inicio, assetids=df['assetid'].unique().tolist(), dts=dts, selected_assetid=assetid, assetid=assetid)

@app.route('/load_data', methods=['POST'])
def load_data():
        global nomes
        global df
        global dfglobal
        global dts
        # Obtém o assetid selecionado no dropdown
        assetid = request.form['assetid']
        datass = request.form['data_selecionada']
        req = request.form['data_selecionada']
        # Filtra o DataFrame pelo assetid selecionado
        asset_trips = dfglobal[dfglobal['assetid'] == assetid]
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
        
        #datas = list(zip(data_formatada2, data_formatada))
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
     
        #datas2 = list(zip(datainicio_filtrada, datafim_filtrada))
        # Convertendo as strings de datas em objetos datetime
        datas1 = [datetime.strptime(data, '%Y-%m-%d %H:%M:%S') for data in datainicio_filtrada]
        datas2 = [datetime.strptime(data, '%Y-%m-%d %H:%M:%S') for data in datafim_filtrada]

        # Organizando as duas listas da data mais recente para a mais antiga
        datas1_recente_ate_antiga = sorted(datas1, reverse=True)
        datas2_recente_ate_antiga = sorted(datas2, reverse=True)

        # Organizando as duas listas da mais antiga para a mais recente
        datas1_antiga_ate_recente = sorted(datas1)
        datas2_antiga_ate_recente = sorted(datas2) 

        # Zip das duas listas organizadas
        datas_organizadas1 = list(zip(datas1_recente_ate_antiga, datas2_recente_ate_antiga))
        datas_organizadas2 = list(zip(datas1_antiga_ate_recente, datas2_antiga_ate_recente))
        
        # Converter as colunas para o tipo datetime
        #df1['tripstart'] = pd.to_datetime(df1['tripstart'])
        #df1['tripstart'] = datetime.fromisoformat(df1['tripstart'])
        # Encontrar a data mais antiga do tripstart e a data mais recente do tripend
        #datainicio = df1['tripstart'].min()
        #print(datainicio)
        return render_template('index.html', assetids=df['assetid'].unique().tolist(),datas1= datas_organizadas1, datas2= datas_organizadas2,dts = dts, selected_data = req, selected_assetid=assetid)


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

    # Obtém o assetid, tripid e a data selecionados no dropdown
    assetid = request.form['assetid']
    datainicial = request.form['data1']
    datafinal = request.form['data2']
    data_formatada1 = datetime.strptime(datainicial, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    data_formatada2 = datetime.strptime(datafinal, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    data_formatada3 = adicionar_tres_horas(data_formatada1)
    data_formatada4 = adicionar_tres_horas(data_formatada2)

    query2 = f'''SELECT * FROM ita_mix.vw_latlong  
                WHERE (timestamp >= '{data_formatada3}') 
                AND (timestamp <= '{data_formatada4}')
                and assetid like '{assetid}'
                order by timestamp ASC'''
    # Query retorna como um DataFrame
    trip_data = wr.athena.read_sql_query(
        query2, database='ita_mix')
   
    # Filtra o DataFrame pelo assetid e tripid selecionados
    #trip_data = df[(df['assetid'] == assetid) & (df['tripid'] == tripid) & df]

    # Verifica se há dados disponíveis após a filtragem
    if not  trip_data.empty:
        # Cria o mapa

        lat = trip_data['latitude_trunc'].iloc[0]
        long = trip_data['longitude_trunc'].iloc[0]

        map = folium.Map(location=[lat, long], zoom_start=18)

        # Traça a rota da viagem selecionada
        coordinates = trip_data[['latitude_trunc',
                                 'longitude_trunc']].values.tolist()
        folium.PolyLine(coordinates, color='lightblue',
                        weight=1.5, opacity=0.5).add_to(map)

        # Cálculo da velocidade média
        # distance = 0.0
        # time = 0.0
        # speeds = []
        # for i in range(1, len(coordinates)):
        #     lat1, lon1 = coordinates[i-1]
        #     lat2, lon2 = coordinates[i]
        #     dx = math.radians(lon2 - lon1) * math.cos(math.radians(lat1))
        #     dy = math.radians(lat2 - lat1)
        #     distance += math.sqrt(dx*dx + dy*dy) * 6371000.0  # Earth radius in meters
        #     time += 1
        #     speed = distance / time
        #     speeds.append(speed)
# __________________________________
        trip_data['speedkilometresperhour'] = round(
            trip_data['speedkilometresperhour'], 0).astype(int)

        for i in range(1, len(coordinates)):

            p = coordinates[i]

            aux = 0

            if aux % 2 == 0:
                if trip_data['speedkilometresperhour'].iloc[i] > 60:
                    folium.CircleMarker(
                        p, tooltip=f"{trip_data['speedkilometresperhour'].iloc[i]} km/h", color='red', radius=2).add_to(map)
                    folium.CircleMarker(
                        p, tooltip=f"{trip_data['speedkilometresperhour'].iloc[i]} km/h", color='white', radius=1).add_to(map)
                else:
                    folium.CircleMarker(
                        p, tooltip=f"{trip_data['speedkilometresperhour'].iloc[i]} km/h", color='blue', radius=2).add_to(map)
                    folium.CircleMarker(
                        p, tooltip=f"{trip_data['speedkilometresperhour'].iloc[i]} km/h", color='white', radius=1).add_to(map)

            aux = aux + 1
# ___________________________________
        lenght_scale = 0.00015  # Fator de escala para latitude e longitude
        sides_scale = 0.000035  # Fator de escala para os lados do polígono
        sides_angle = 35  # Ângulo para calcular os pontos laterais do polígono

        # Itera sobre os dados da viagem, começando do índice 1 e terminando antes do último índice
        for i in range(1, len(trip_data) - 1):
            if i % 50 == 0:  # Verifica se i é divisível por 50
                latA = trip_data['latitude_trunc'].iloc[i]  # Latitude do ponto A
                longA = trip_data['longitude_trunc'].iloc[i]  # Longitude do ponto A
                heading = trip_data['heading'].iloc[i]  # Ângulo de direção

                # Calcula a latitude e longitude do ponto B com base na escala e no ângulo de direção
                latB = lenght_scale * math.cos(math.radians(heading)) + latA
                longB = lenght_scale * math.sin(math.radians(heading)) + longA

                # Calcula a latitude e longitude do ponto C com base na escala, ângulo de direção e ângulo lateral
                latC = sides_scale * math.cos(math.radians(heading + 180 - sides_angle)) + latB
                longC = sides_scale * math.sin(math.radians(heading + 180 - sides_angle)) + longB

                # Calcula a latitude e longitude do ponto D com base na escala, ângulo de direção e ângulo lateral
                latD = sides_scale * math.cos(math.radians(heading + 180 + sides_angle)) + latB
                longD = sides_scale * math.sin(math.radians(heading + 180 + sides_angle)) + longB

                pointA = (latA, longA)  # Coordenadas do ponto A
                pointB = (latB, longB)  # Coordenadas do ponto B
                pointC = (latC, longC)  # Coordenadas do ponto C
                pointD = (latD, longD)  # Coordenadas do ponto D

                points = [pointA, pointB, pointC, pointD, pointB]  # Lista de pontos que define o polígono

                if trip_data['speedkilometresperhour'].iloc[i] > 60:
                    folium.Polygon(locations=points, color="red").add_to(map)  # Adiciona um polígono vermelho ao mapa se a velocidade for maior que 60 km/h
                else:
                    folium.Polygon(locations=points, color="blue").add_to(map)  # Adiciona um polígono azul ao mapa se a velocidade for menor ou igual a 60 km/h

# ______________________________________
        # avg_speed = sum(speeds) / len(speeds) if speeds else 0.0

        # Adiciona marcadores para início e fim da viagem
        start_location = (
            trip_data['latitude_trunc'].iloc[0], trip_data['longitude_trunc'].iloc[0])
        end_location = (trip_data['latitude_trunc'].iloc[-1], trip_data['longitude_trunc'].iloc[-1])
        # Exibe a velocidade atual no mapa
        # current_speed = speeds[-1] if speeds else 0.0
        # folium.Marker(end_location, tooltip=f'Current Speed: {current_speed:.2f} m/s').add_to(map)

        # Adiciona as médias de velocidade e velocidades em tempo real no mapa
        # folium.Marker(start_location, tooltip=f'Average Speed: {avg_speed:.2f} m/s').add_to(map)

        # Adiciona a imagem ao popup final da viagem
        #image_html = f"<img src='{trip_data['assetimageurl'].iloc[-1]}' width='200px'>"
       
        
        popup_inicio = f"Inicio da viagem:{data_formatada1}\n Asset:{assetid}"

        popup_fim = f"Final da viagem:{data_formatada2}\n Asset:{assetid}"

        folium.Marker(start_location, tooltip='Start', popup=popup_inicio,
                      icon=folium.Icon(color="lightblue", icon="flag")).add_to(map)
        folium.Marker(end_location, tooltip='End', popup=popup_fim, icon=folium.Icon(
            color="lightblue", icon="screenshot")).add_to(map)

        
    
        return map._repr_html_()
    
    
    
    else:
        return render_template('index.html',timestamp= trip_data['timestamp'],erro='ok',selected_assetid = assetid,  message='No data available for the selected trip')


if __name__ == '__main__':
    app.run(debug=True)
