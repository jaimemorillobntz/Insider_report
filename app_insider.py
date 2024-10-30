import streamlit as st
import pandas as pd
from Insider_trading_secrets import obtener_transacciones_insiders, dividir_compras_ventas, filtrar_por_fecha, formatear_fecha, obtener_acciones_totales, crear_resumen

# Título de la aplicación
st.title('Análisis de Transacciones de Insiders')

# Instrucciones para el usuario
st.write("""
    Esta aplicación permite visualizar las transacciones de insiders de empresas listadas.
    Selecciona el ticker de la empresa y el número de días para filtrar las transacciones más recientes.
""")

# Entrada de usuario: seleccionar tickers
tickers_input = st.text_input(
    "Introduce los tickers separados por comas (por ejemplo: AAPL, TSLA, GOOGL)",
    value='AAPL, TSLA, GOOGL'
)

# Convertir la entrada en una lista de tickers
tickers = [ticker.strip().upper() for ticker in tickers_input.split(',')]

# Entrada de usuario: seleccionar número de días para filtrar
dias_input = st.number_input(
    "Número de días para filtrar las transacciones recientes:",
    min_value=1, max_value=365, value=15, step=1
)

# Botón para cargar datos
if st.button('Cargar datos'):
    # Mostrar un mensaje de carga
    st.write("Obteniendo datos, por favor espera...")

    # Obtener transacciones de múltiples tickers y combinarlas
    df_total = pd.DataFrame()
    for ticker in tickers:
        df_ticker = obtener_transacciones_insiders(ticker)
        df_total = pd.concat([df_total, df_ticker], ignore_index=True)
    
    # Si hay datos, procesar y mostrar
    if not df_total.empty:
        # Dividir en compras y ventas
        df_compras, df_ventas = dividir_compras_ventas(df_total)
        
        # Filtrar las transacciones por fecha
        df_compras = filtrar_por_fecha(df_compras, dias=dias_input)
        df_ventas = filtrar_por_fecha(df_ventas, dias=dias_input)
        
        # Formatear fechas
        df_compras = formatear_fecha(df_compras)
        df_ventas = formatear_fecha(df_ventas)
        
        # Crear resúmenes
        total_acciones = obtener_acciones_totales(tickers)
        resumen_compras, resumen_ventas = crear_resumen(df_compras, df_ventas, total_acciones)
        
        # Eliminar índices antes de mostrar
        df_compras = df_compras.reset_index(drop=True)
        df_ventas = df_ventas.reset_index(drop=True)
        resumen_compras = resumen_compras.reset_index(drop=True)
        resumen_ventas = resumen_ventas.reset_index(drop=True)
        
        # Mostrar los DataFrames en la aplicación
        st.subheader("Transacciones de Compras")
        st.dataframe(df_compras)

        st.subheader("Transacciones de Ventas")
        st.dataframe(df_ventas)

        st.subheader("Resumen de Compras")
        st.dataframe(resumen_compras)

        st.subheader("Resumen de Ventas")
        st.dataframe(resumen_ventas)
else:
    st.warning("No se encontraron transacciones para los tickers seleccionados o los directivos de tu empresa no tienen que rellenar el formulario de la SEC.")

