import meteostat as ms
import datetime as dt
import duckdb
import warnings

def save_parquet(conn):
    # Consulta todos os dados recuperados, renomeando as colunas
    query = "SELECT id as ID, station as Estacao, tavg as Temp_Media, tmin as Temp_Min, tmax as Temp_Max, prcp as Precipitacao, snow as Neve, wdir as Vento_Dir, wspd as Vento_Med, wpgt as Vento_Max, pres as Pressao, tsun as Tempo_Sol FROM weather_data"

    # Executa a consulta
    result = conn.execute(query).fetchdf()

    #Salvando em parquet
    result.to_parquet('data/measurements_summary.parquet')

def get_data(start, end, conn, region, debug, deleteTable):
    if region:
        # Obtendo todas as estações disponíveis para a região definida (Brasil)
        stations = ms.Stations().region('BR')
    else:
        # Obtendo todas as estações disponíveis
        stations = ms.Stations()

    # Buscar as estações
    all_stations = stations.fetch()

    # Este comando cria a tabela, mas não insere dados, garantindo que os campos sejam configurados corretamente.
    sample_station = ms.Daily(all_stations.index[0], start, end).fetch()  # Buscar dados para uma estação amostra
    sample_station['id'] = all_stations.index[0]  # Adicionar a coluna de 'id' de exemplo para definir o esquema
    sample_station['station'] = all_stations.index[0]  # Adicionar a coluna de 'station' de exemplo para definir o esquema

    if deleteTable:
        # Deleta a tabela
        conn.execute("DROP TABLE IF EXISTS weather_data")
    # Tenta criar a tabela apenas uma vez por execução do ETL
    conn.execute("CREATE TABLE IF NOT EXISTS weather_data AS SELECT * FROM sample_station LIMIT 0")

    if debug:
        size = all_stations.index.size # Quantidade de estações buscadas
        count = 1 # Inicia um contador

    # Suprime avisos relacionados ao uso de datas pelo Pandas na biblioteca do Meteostat
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    # Iterar por cada estação e buscar dados diários
    for station, station_info in all_stations.iterrows():
        # Busca os dados da estação para o período definido
        data = ms.Daily(station, start, end)
        data = data.fetch()

        # Se retornou algum dado
        if not data.empty:
            # ID da estação. Retornado pela biblioteca
            data['id'] = station

            # Obter o nome da cidade. Retornado pela biblioteca
            data['station'] = station_info['name']

            # Salvar no banco de dados DuckDB
            conn.execute("INSERT INTO weather_data SELECT * FROM data")

            if debug:
                print(count) # Contador
        elif debug:
            # Caso não tenha retornado os dados
            print(f"Ñ {count} de {size}")
        
        if debug:
            count = count + 1 # Acréscimo do contador

if __name__ == "__main__":
    import time

    # Modo debug: imprime informações na tela
    debug = False
    debug = True

    # Modo debug: imprime informações na tela
    execTime = False
    execTime = True
    
    # Data final da busca (hoje)
    end = dt.datetime.now()
    # Data inicial da busca (x dias, semanas)
    start = end - dt.timedelta(days=1)
    
    # Define região
    region = ''
    region = 'BR'

    if execTime:
        print("Iniciando o processamento do arquivo.")
        start_time = time.time()

    # Conecta com o duckdb
    conn = duckdb.connect('weather_data.duckdb')

    # Busca os dados do Meteostat
    get_data(start=start, end=end, conn=conn, region=region, debug=debug, deleteTable=True)

    # Salva os dados recuperados no arquivo parquet
    save_parquet(conn=conn)

    if execTime:
        took = time.time() - start_time
        print(f"Processamento levou {took:.2f} segundos.")

    print("Successful")