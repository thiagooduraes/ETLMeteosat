import streamlit as st
import duckdb
import pandas as pd

def load_data():
    con = duckdb.connect()
    df = con.execute("SELECT * FROM '../data/measurements_summary.parquet'").df()
    con.close()
    return df

def main():
    st.title("Weather Station Summary")
    st.write("Esse Dashboard mostra um resumo das temperaturas")

    data = load_data()

    st.dataframe(data)

if __name__ == "__main__":
    main()