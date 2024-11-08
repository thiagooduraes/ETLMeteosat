import meteostat as ms
import datetime as dt
import duckdb
import warnings

def execute_duckdb(conn):
    query = "SELECT id as ID, station as Estacao, tavg as Temp_Media, tmin as Temp_Min, tmax as Temp_Max, prcp as Precipitacao, snow as Neve, wdir as Vento_Dir, wspd as Vento_Med, wpgt as Vento_Max, pres as Pressao, tsun as Tempo_Sol FROM weather_data"

    result = conn.execute(query).fetchdf()

    #Sresult.show()

    #Salvando em arquivo parquet
    result.to_parquet('data/measurements_summary.parquet')

if __name__ == "__main__":
    import time
    
    # Set time period
    end = dt.datetime.now()
    start = end - dt.timedelta(days=1)

    # Obtendo todas as estações disponíveis
    stations = ms.Stations().region('BR')
    # Aplicar filtros para os dados desejados (opcional)
    # Por exemplo, filtrar por data de início de operação e localização

    # Buscar as estações
    all_stations = stations.fetch()

    conn = duckdb.connect('weather_data.duckdb')
    
    print("Iniciando o processamento do arquivo.")
    start_time = time.time()

    # Criar a tabela apenas uma vez
    # Este comando cria a tabela, mas não insere dados, garantindo que os campos sejam configurados corretamente.
    sample_station = ms.Daily(all_stations.index[0], start, end).fetch()  # Buscar dados para uma estação amostra
    sample_station['id'] = all_stations.index[0]  # Adicionar a coluna de 'station' de exemplo para definir o esquema
    sample_station['station'] = all_stations.index[0]  # Adicionar a coluna de 'city_name' de exemplo para definir o esquema

    conn.execute("CREATE TABLE IF NOT EXISTS weather_data AS SELECT * FROM sample_station LIMIT 0")


    # Iterar por cada estação e buscar dados diários
    size = all_stations.index.size
    count = 1
    warnings.filterwarnings("ignore", category=FutureWarning)
    for station, station_info in all_stations.iterrows():
        data = ms.Daily(station, start, end)
        data = data.fetch()
        if not data.empty:
            data['id'] = station

            # Obter o nome da cidade
            data['station'] = station_info['name']
            # Salvar no banco de dados DuckDB
            conn.execute("INSERT INTO weather_data SELECT * FROM data")
            print(count)
        else:
            print(f"Ñ {count} de {size}")
        count = count + 1

    execute_duckdb(conn=conn)

    took = time.time() - start_time
    
    print(f"Processamento levou {took:.2f} segundos.")