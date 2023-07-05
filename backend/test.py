#Bibliotecas
import boto3
import awswrangler as wr


# --------- Funções para suporte do ETL ---------
#Variáveis para acesso a AWS


session = boto3.Session(aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      region_name=aws_region)

#FUNÇÕES PARA CRIAR O SCHEMA DOS PARQUETS


try:  
# Exemplo de uso da função

    garagem =  wr.s3.read_parquet(path= 's3://ita-silver/mix/garagem/mix_subgroups.parquet', boto3_session = session)
    #print(garagem.columns)
    asseto =  wr.s3.read_parquet(path= 's3://ita-silver/mix/asset_to_garage/asset_to_garage.parquet', boto3_session = session)
    #print(asseto.columns)
    site = garagem[garagem['Type'].str.contains('SiteGroup')]
    #print(len(site) )
    # Filtrar o DataFrame com base na condição (NOT (type LIKE '%SiteGroup%'))
    org = garagem[~garagem['Type'].str.contains('SiteGroup')]
    #print(len(org) )
    # Filtrando os dataframes conforme a condição WHERE da consulta

  
    assetto = asseto[asseto['UserState'].str.contains('Available')]
    
    print(site.columns)
    sitee = site[site['Name'].str.contains('ITA Transportes')]
    orgg = org[org['Name'].str.contains('ITA Transportes') | org['NameMae'].str.contains('ITA Transportes')]
    print(sitee)
    #print(site)
    #print(asseto)
  
    # Mesclando os dataframes
    df_final = (
        asseto.merge(site, left_on='siteid', right_on='GroupId')
                    .merge(org, left_on='GroupId', right_on='GaragemMae')
    )

    

    # Exibindo o resultado
    #print(df_final.head())
except Exception as erro:
    print(erro)
