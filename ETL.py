import meteostat
import datetime as dt
import duckdb

def create_duckdb():
    result = duckdb.sql("""
    SELECT 
    tavg as Temp_Media, tmin as Temp_Min,
    tmax as Temp_Max,
    prcp as Precipitacao,
    snow as Neve,
    wdir as Vento_Dir,
    wspd as Vento_Med,
    wpgt as Vento_Max,
    pres as Pressao,
    tsun as Tempo_Sol
    FROM data
    """)

    result.show()

    #Salvando em arquivo parquet
    result.write_parquet('data/measurements_summary.parquet')

if __name__ == "__main__":
    import time
    
    # Set time period
    end = dt.datetime.now()
    start = end - dt.timedelta(weeks=1)

    # Create Point for Montes Claros, MG
    location = meteostat.Point(-16.7167, -43.8667, 646)

    # Get daily data for 2018
    data = meteostat.Daily(location, start, end)
    data = data.fetch()

    # Plot line chart including average, minimum and maximum temperature
    data.plot(y=['tavg', 'tmin', 'tmax'])
    # station, time, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun

    print("Iniciando o processamento do arquivo.")
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    
    print(f"Processamento levou {took:.2f} segundos.")