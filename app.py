from flask import Flask, render_template, request
from flask_caching import Cache
import folium
import pandas as pd
import boto3
import awswrangler as wr
import math
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import def_dates
import def_querys
# Obtém o valor da variável de ambiente API_KEY
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')
aws_region = os.getenv('aws_region')


# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


nomes=['hemolabor',
       'jaime câmara'
       ]
# nomes = [
#     'enel',
#     'jaime câmara',
#     'aqualis',
#     'car sharing',
#     'carros por assinatura',
#     'chaves identificação',
#     'coming',
#     'elcop',
#     'comurg',
#     'prefeitura de goiânia',
#     'estoque ita',
#     'express reforma',
#     'hemolabor',
#     'saneago',
#     'csc',
#     'manutenção',
#     'cahves ita geral',
#     'last mile',
#     'itapar',
#     'líder pets',
#     'pdca engenharia',
#     'triunfo concebra',
#     'urban tecnologia',
#     'Ultra Solar',
#     'Tribunal de Contas',
#     'Tencel Engenharia',
#     'Teclog Fleet',
#     'Suporte Sondagens Investigações',
#     'Serviços - ADM',
#     'SEPLAD/DF',
#     'Rotagyn',
#     'Redemob Consorcio',
#     'Prospec Cell',
#     'PRIMETEK',
#     'Palme Vendas',
#     'Outlet Primaveira',
#     'Moriá Prestação de Serviços',
#     'Líder Fomento Comercial',
#     'IPEM - Instituto de Pesos e Medidas',
#     'HSI Serviços e Comércio de Pneus',
#     'HP Transportes Coletivos',
#     'GOINFRA - Agência Goiana De Infr. E Transportes',
#     'Goiás Rendering',
#     'Goiás Minas|ITALAC',
#     'Geogis Geotecnologia Ltda',
#     'GAV - Pirenópolis Empr. Imob',
#     'Franca e Pereira Ltda',
#     'FAPEG - Fundação De Amparo Pesq. Est. Goiás',
#     'Enebra',
#     'Enapa Empresa Nac. Pavimentação',
#     'DS Facility Ltda',
#     'Dolp Engenharia',
#     'DEC Agro Comercio e representações LTDA',
#     'D A Tecnologia e Serviços',
#     'Confrex Tec. Veicular',
#     'Concebra',
#     'Bold Ent',
#     'Barão Especialidades',
#     'Ambiente Consultoria',
#     'Agromais Agropecuária'
# ]

  
    
    
    
app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


#clientip = None




pasta_temporaria = 'temp/'

# Define o caminho para o arquivo temporário local

#caminho_temporario_local = os.path.join(tempfile.gettempdir(), os.path.basename(arquivo_gz))

#print(caminho_temporario_local)



# Exclui o arquivo temporário local após a leitura

#os.remove(caminho_temporario_local)




# Configura a região da sessão do cliente boto3
boto3.setup_default_session(aws_access_key_id= aws_access_key_id,
                            aws_secret_access_key= aws_secret_access_key,
                            region_name=aws_region)

# Executa a consulta no Athena


@app.route('/')
def index():
    global nomes
    return render_template('index.html', nomes=nomes)
@app.route('/nome', methods=[ 'POST'])
def nome():
    global nomes
    nome = request.form['client']
    return render_template('index.html',selected_nome=nome,variavelcontrole1='ok', nomes=nomes)
# @app.route('/escolha', methods=[ 'POST'])
# def escolha():
#     nome = request.form['client']
#     escolha = request.form.get('escolha')
#     if(escolha == 'data'):
#         return render_template('index.html',selected_nome=nome, nomes=nomes, variavelcontrole1='ok')
#     if(escolha == 'clientes'):
#         return render_template('index.html',selected_nome=nome, nomes=nomes, variavelcontrole2='ok')




###################ESCOLHA DATA###############################################################################################################


@app.route('/veiculos', methods=[ 'POST'])
def veiculos():
    global nomes
    nome = request.form['client']
    df = def_querys.execute_query(nome) 
    #Obtém a lista de assetids únicos
    assetids = df['assetid'].unique().tolist()
    dia = request.form['data_selecionada']
    data = datetime.strptime(dia, '%Y-%m-%d')
    datatz = data.strftime('%Y-%m-%d %H:%M:%S')#datainicio
    # Adicionando um dia
    data_nova = data + timedelta(days=1)
    datatz2 = data_nova.strftime('%Y-%m-%d %H:%M:%S')#dataumdia a mais
    if nome == "hemolabor":
        cliente = "_hemolabor"
    if nome == "jaime câmara":
        cliente = "jaime_camara"

    df2 =  def_querys.execute_query2(assetids , datatz, datatz2,nome ,cliente)
    if df2.empty:
        return render_template("error.html")

    
    variavelcontroleveiculo = "ok"
    assetids2 = df2['assetid'].unique().tolist()
    descricao = df2['description'].unique().tolist()
    assetplaca = list(zip(descricao, assetids2))
    return render_template('index.html',variavelcontroleveiculo = variavelcontroleveiculo, assetids=assetids2,selected_data = dia, descricao = assetplaca, selected_nome=nome, nomes=nomes, dia=dia, variavelcontrole1='ok')




@app.route('/selectdata', methods=['POST'])
def selectdata():
        
        
        #Obtém a lista de assetids únicos
      
        nome = request.form['client']
        df = def_querys.execute_query(nome)  
        assetids = df['assetid'].unique().tolist()
        assetid = request.form['assetid']
        dia = request.form['data_selecionada']
        data = datetime.strptime(dia, '%Y-%m-%d')
        datatz = data.strftime('%Y-%m-%d %H:%M:%S')#datainicio
        # Adicionando um dia
        data_nova = data + timedelta(days=1)
        datatz2 = data_nova.strftime('%Y-%m-%d %H:%M:%S')#dataumdia a mais
        # Obtém a lista de assetids únicos
        if nome == "hemolabor":
            cliente = "_hemolabor"
        if nome == "jaime câmara":
            cliente = "jaime_camara"
        df2 =  def_querys.execute_query2(assetids , datatz, datatz2, nome, cliente)
        if df2.empty:
         return render_template("error.html")
        
        assetids2 = df2['assetid'].unique().tolist()
        descricao = df2['description'].unique().tolist()
        assetplaca = list(zip(descricao, assetids2))
        imagem = df2['assetimageurl'].unique().tolist()
        assetplaca = list(zip(descricao, assetids2))
        global nomes
        dfglobal= df2[df2['assetid'] == assetid]
         
        # Filtra o DataFrame pelo assetid selecionado
        asset_trips = dfglobal[dfglobal['assetid'] == assetid]
        datainicio = asset_trips['tripstart'].unique().tolist()
        minimo = asset_trips['tripstart'].min()
        maximo = asset_trips['tripend'].max()
        datafim = asset_trips['tripend'].unique().tolist()
        data_formatada = []  # Lista para armazenar as datas formatadas
        minimo = def_dates.converter_fuso_horario(str(minimo))
        maximo = def_dates.converter_fuso_horario(str(maximo))
        for data in datafim:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada.append(def_dates.converter_fuso_horario(str(data)))
        data_formatada2 = []  # Lista para armazenar as datas formatadas

        for data in datainicio:
            # Aplica a função de conversão de fuso horário para cada data
            data_formatada2.append(def_dates.converter_fuso_horario(str(data)))
        
        #datas = list(zip(data_formatada2, data_formatada))
        datainicio_filtrada = []

        # Converter a variável 'datass' em um objeto datetime
        dataass_dt = datetime.strptime(dia, '%Y-%m-%d').date()

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
        variavelcontroleveiculo= "ok"
        # Zip das duas listas organizadas
        datas_organizadas1 = list(zip(datas1_recente_ate_antiga, datas2_recente_ate_antiga))
        datas_organizadas2 = list(zip(datas1_antiga_ate_recente, datas2_antiga_ate_recente))
        variavelcontroleviagem ="ok"
        # Converter as colunas para o tipo datetime
        #df1['tripstart'] = pd.to_datetime(df1['tripstart'])
        #df1['tripstart'] = datetime.fromisoformat(df1['tripstart'])
        # Encontrar a data mais antiga do tripstart e a data mais recente do tripend
        #datainicio = df1['tripstart'].min()
        #print(datainicio)
        return render_template('index.html',variavelcontroleviagem =variavelcontroleviagem,variavelcontroleveiculo = variavelcontroleveiculo, minimo =minimo, maximo = maximo, descricao=assetplaca, selected_nome= nome,nomes=nomes,datas0=datas_organizadas1, datas1= datas_organizadas1, datas2= datas_organizadas2, selected_data = dia, selected_assetid=assetid, variavelcontrole1='ok')

@app.route('/plot', methods=['POST'])
def plot():
    dia = request.form['data_selecionada']
    clientip = request.remote_addr    
    global nomes
    minimo = request.form['inicio']
    maximo = request.form['fim']
    # Converter para o formato desejado
    minimotz = def_dates.adicionar_tres_horas(minimo)
    maximotz = def_dates.adicionar_tres_horas(maximo)
    minimotz = datetime.strptime(minimotz, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    maximotz = datetime.strptime(maximotz, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    # Obtém o assetid, tripid e a data selecionados no dropdown
    assetid = request.form['assetid']
    nome = request.form['client']
    datainicial = request.form['data1']
    datafinal = request.form['data2']
    #data_formatada1 = datetime.strptime(datainicial, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    #data_formatada2 = datetime.strptime(datafinal, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ')
    #data_formatada3 = adicionar_tres_horas(data_formatada1)
    #data_formatada4 = adicionar_tres_horas(data_formatada2)
    #df = execute_query(nome, clientip = clientip)
    

    if nome =="hemolabor":
        cliente = "_hemolabor"
        query2= f'''SELECT *, CAST(speedkilometresperhour AS DOUBLE) AS speedkilometresperhours,
CAST(heading AS DOUBLE) AS headings FROM  _hemolabor.latitudelongitude
                WHERE (timestamp >= '{minimotz}') 
                AND (timestamp <= '{maximotz}')
                and assetid like '{assetid}'
                order by timestamp ASC'''
        queryevento = f'''
        SELECT * FROM "_hemolabor"."eventos" 
where lower(eventtypeid) like '%-2864371101042705250%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
''' 
        queryevento2 = f'''
        SELECT * FROM "ita_mix"."eventos" 
where lower(eventtypeid) like '-7025578424515401815%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
''' 
        queryevento3 = f'''
        SELECT * FROM "ita_mix"."eventos" 
where lower(eventtypeid) like '4585574725405088119%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
''' 
        print(queryevento2)
        event2= wr.athena.read_sql_query(queryevento2, database=f'ita_mix', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
        #print(queryevento3)
        #event3= wr.athena.read_sql_query(queryevento3, database=f'ita_mix', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
        #event= wr.athena.read_sql_query(queryevento, database=f'_hemolabor', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
        print(event2.head())
        #print(event3.head())
    if nome =="jaime câmara":
        query2= f'''SELECT *, CAST(speedkilometresperhour AS DOUBLE) AS speedkilometresperhours,
CAST(heading AS DOUBLE) AS headings FROM  jaime_camara.latitudelongitude
                WHERE (timestamp >= '{minimotz}') 
                AND (timestamp <= '{maximotz}')
                and assetid like '{assetid}'
                order by timestamp ASC''' 
        queryevento = f'''
        SELECT * FROM "jaime_camara"."eventos" 
where lower(eventtypeid) like '%-2864371101042705250%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
'''

        queryevento2 = f'''
        SELECT * FROM "ita_mix"."eventos" 
where lower(eventtypeid) like '-7025578424515401815%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
''' 
        queryevento3 = f'''
        SELECT * FROM "ita_mix"."eventos" 
where lower(eventtypeid) like '4585574725405088119%'
and assetid like '{assetid}'
and startdatetime>= '{minimotz}'
and startdatetime <= '{maximotz}' 
''' 
        print(queryevento2)
        event2= wr.athena.read_sql_query(queryevento2, database=f'{cliente}', s3_output='s3://ita-athena-queue/py/')
        #print(queryevento3)
        #event3= wr.athena.read_sql_query(queryevento3, database=f'ita_mix', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
        #event= wr.athena.read_sql_query(queryevento, database=f'jaime_camara', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})

    print(query2)
    #print(queryevento)         
    # Query retorna como um DataFrame
    #event= wr.athena.read_sql_query(
        #queryevento, database=f'_hemolabor', s3_output='s3://ita-athena-queue/py/', athena_cache_settings={"max_cache_seconds": 900, "max_cache_query_inspections": 1000})
    trip_data = wr.athena.read_sql_query(
        query2, database='ita_mix', s3_output='s3://ita-athena-queue/py/')
    # print(event.head())
    # if not event.empty:
    #     event['startdatetime'] = pd.to_datetime(event['startdatetime'])
    #     event['enddatetime'] = pd.to_datetime(event['enddatetime'])
    print(trip_data.head())
    # Supondo que sua coluna "timestamp" esteja no formato de string, converta-a para o formato datetime.
    trip_data['timestamp'] = pd.to_datetime(trip_data['timestamp'])
    event2['startdatetime'] = pd.to_datetime(event2['startdatetime'])

    # Classifique o DataFrame com base na coluna "timestamp" em ordem crescente.
    trip_data = trip_data.sort_values(by='timestamp')
    # Filtra o DataFrame pelo assetid e tripid selecionados
    #trip_data = df[(df['assetid'] == assetid) & (df['tripid'] == tripid) & df]
    trip_data['speedlimits'] = trip_data['speedlimit'].astype(float)
    
    df = def_querys.execute_query(nome)
    
    # Obtém a lista de assetids únicos
    assetids = df['assetid'].unique().tolist()
    descricao = df['description'].unique().tolist()
    imagem = df['assetimageurl'].unique().tolist()
    assetplaca = list(zip(descricao, assetids))
    
    datames= def_dates.formatar_data()#importando dts
   
    # Verifica se há dados disponíveis após a filtragem
    if not  trip_data.empty:
        # Cria o mapa

        lat = trip_data['latitude_trunc'].iloc[0]
        long = trip_data['longitude_trunc'].iloc[0]

        map = folium.Map(location=[lat, long], zoom_start=14)

        # Traça a rota da viagem selecionada
        coordinates = trip_data[['latitude_trunc',
                                 'longitude_trunc']].values.tolist()
        folium.PolyLine(coordinates, color='lightblue',
                        weight=1.5, opacity=0.5).add_to(map)
        
        trip_data['speedkilometresperhours'] = round(
            trip_data['speedkilometresperhours'], 0).astype(int)
        for i in range(1, len(coordinates)):
            condicao_atendida = False
            p = coordinates[i]
            if(trip_data['speedlimits'].iloc[i]!= None):               
                if(trip_data['speedkilometresperhours'].iloc[i]> trip_data['speedlimits'].iloc[i]):
                    print("entrou no meu if")
                    trip_data['timestamp'].iloc[i] = def_dates.converter_fuso_horario3(str(trip_data['timestamp'].iloc[i]))
                    trip_data['timestamp'].iloc[i] = pd.to_datetime(trip_data['timestamp'].iloc[i])
                    condicao_atendida = True
                    folium.CircleMarker(p, tooltip=f"velocidade: {trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br>  data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color='red', radius=0.5).add_to(map)
               
                    
            # for j in range(1,len(event)):
            #     if trip_data['timestamp'].iloc[i]>=event['startdatetime'].iloc[j] and trip_data['timestamp'].iloc[i]<=event['enddatetime'].iloc[j]:
            #                             print("entrou no if")
            #                             trip_data['timestamp'].iloc[i] = def_dates.converter_fuso_horario3(str(trip_data['timestamp'].iloc[i]))
            #                             trip_data['timestamp'].iloc[i] = pd.to_datetime(trip_data['timestamp'].iloc[i])
            #                             condicao_atendida = True
            #                             folium.CircleMarker(
            #                                 p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color='red', radius=0.5).add_to(map)
                                        
            #                             break
            if event2 is not False:
                for u in range(1,len(event2)):
                    #   print(f"tempolatlong{trip_data['timestamp'].iloc[i]}")
                    #   print(event2['startdatetime'].iloc[u])
                    if trip_data['timestamp'].iloc[i]==event2['startdatetime'].iloc[u]:
                                            popup=f"velocidade: {trip_data['speedkilometresperhours'].iloc[i]} km/h <br> limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}"
                                            html_popup = f"""
                                            <div style="background-color: #FF0000;padding-left:0px;width: 110%;hight:150%; border: 10px solid #FF0000; color: #FFFFFF; padding: 10px; border-radius: 15px;">
                                                <h4>{popup}</h4>
                                                <h6>Evento: Curva Brusca</h6>
                                            </div>
                                        """
                                            print("entrou no if event2")
                                            trip_data['timestamp'].iloc[i] = def_dates.converter_fuso_horario3(str(trip_data['timestamp'].iloc[i]))
                                            trip_data['timestamp'].iloc[i] = pd.to_datetime(trip_data['timestamp'].iloc[i])
                                            condicao_atendida = True
                                            folium.Marker(p,  popup=folium.Popup(html_popup, max_width=300), icon=folium.Icon(color='red')).add_to(map)
                                            break
            # if event3 is not False:
            #     for p in range(1,len(event3)):
            #         #(f"tempolatlong{trip_data['timestamp'].iloc[i]}")
            #         #print(event2['startdatetime'].iloc[p])
            #         if trip_data['timestamp'].iloc[i]>=event3['startdatetime'].iloc[p] and trip_data['timestamp'].iloc[i]<=event3['enddatetime'].iloc[p]:
            #                                 print("entrou no if")
            #                                 trip_data['timestamp'].iloc[i] = def_dates.converter_fuso_horario3(str(trip_data['timestamp'].iloc[i]))
            #                                 trip_data['timestamp'].iloc[i] = pd.to_datetime(trip_data['timestamp'].iloc[i])
            #                                 condicao_atendida = True
            #                                 folium.CircleMarker(
            #                                     p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color='yellow', radius=10).add_to(map)
                                            
            #                                 break
            if not condicao_atendida:
                trip_data['timestamp'].iloc[i] = def_dates.converter_fuso_horario3(str(trip_data['timestamp'].iloc[i]))
                trip_data['timestamp'].iloc[i] = pd.to_datetime(trip_data['timestamp'].iloc[i])
                folium.CircleMarker(
                        p, tooltip=f"velocidade: {trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color='blue', radius=0.5).add_to(map)
                                               
        
        # for i in range(1, len(coordinates)):

        #     p = coordinates[i]
 
        #     aux = 0

        #     if aux % 2 == 0:
        #         if trip_data['speedkilometresperhours'].iloc[i] > 60:
        #             folium.CircleMarker(
        #                 p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h", color='blue', radius=2).add_to(map)
        #             folium.CircleMarker(
        #                 p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h", color='white', radius=1).add_to(map)
        #         else:
        #             folium.CircleMarker(
        #                 p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h", color='blue', radius=2).add_to(map)
        #             folium.CircleMarker(
        #                 p, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h", color='white', radius=1).add_to(map)

        #     aux = aux + 1
# ___________________________________
        lenght_scale = 0.00015  # Fator de escala para latitude e longitude
        sides_scale = 0.000035  # Fator de escala para os lados do polígono
        sides_angle = 35  # Ângulo para calcular os pontos laterais do polígono

        # Itera sobre os dados da viagem, começando do índice 1 e terminando antes do último índice
        for i in range(1, len(trip_data) - 1):
            condicao_atendida = False
            if i % 1 == 0:  # Verifica se i é divisível por 50
                latA = trip_data['latitude_trunc'].iloc[i]  # Latitude do ponto A
                longA = trip_data['longitude_trunc'].iloc[i]  # Longitude do ponto A
                heading = trip_data['headings'].iloc[i]  # Ângulo de direção

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
                # for j in range(1,len(event)):
                #     if trip_data['timestamp'].iloc[i]>=event['startdatetime'].iloc[j] and trip_data['timestamp'].iloc[i]<=event['enddatetime'].iloc[j]:
                #         folium.Polygon(locations=points, tooltip=f"{trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color="red").add_to(map)  # Adiciona um polígono vermelho ao mapa se a velocidade for maior que 60 km/h
                #         condicao_atendida = True
                #         break
                if not condicao_atendida and (trip_data['speedlimits'].iloc[i] is not None):
                     if(trip_data['speedkilometresperhours'].iloc[i]> trip_data['speedlimits'].iloc[i]):                       
                        folium.Polygon(locations=points, 
                                           tooltip=f"velocidade: {trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", 
                                           color="red").add_to(map)  # Adiciona um polígono vermelho ao mapa se a velocidade for maior que 60 km/h
                        condicao_atendida = True
                    # Se nenhuma das duas condições foi atendida, entra no else
                if not condicao_atendida:
                    folium.Polygon(locations=points, tooltip=f"velocidade: {trip_data['speedkilometresperhours'].iloc[i]} km/h <br>limit via:{trip_data['speedlimit'].iloc[i]}km/h<br> data:{trip_data['timestamp'].iloc[i].strftime('%H:%M:%S')}", color="blue").add_to(map)


# ______________________________________
        # avg_speed = sum(speeds) / len(speeds) if speeds else 0.0

        # Adiciona marcadores para início e fim da viagem
        start_location = (
            trip_data['latitude_trunc'].iloc[0], trip_data['longitude_trunc'].iloc[0])
        end_location = (trip_data['latitude_trunc'].iloc[-1], trip_data['longitude_trunc'].iloc[-1])

        # Adiciona a imagem ao popup final da viagem
        dfimg = df[df['assetid'] == assetid]
        #image_html = f"<img src='{trip_data['assetimageurl'].iloc[-1]}' width='200px'>"
        image_html = f"<img src='{dfimg['assetimageurl'].iloc[-1]}' width='200px'>"
        
        popup_inicio = f"Inicio da viagem:{minimo}\n Asset:{assetid}\n {image_html}"

        popup_fim = f"Final da viagem:{maximo}\n Asset:{assetid}\n {image_html}"

        folium.Marker(start_location, popup=popup_inicio, tooltip='Start', 
                      icon=folium.Icon(color="lightblue", icon="flag")).add_to(map)
        folium.Marker(end_location, popup=popup_fim, tooltip='End',  icon=folium.Icon(
            color="lightblue", icon="screenshot")).add_to(map) 
    
        return map._repr_html_() 
    
    else:
        return render_template('index.html',minimo=minimo, maximo = maximo,
         descricao = assetplaca, selected_nome = nome,
        nomes = nomes ,erro='ok',selected_assetid = assetid, variavelcontrole1="ok",
        selected_data=dia, message='No data available for the selected trip')

if __name__ == '__main__':   
    app.run(host='0.0.0.0', port=8000, debug=True)
