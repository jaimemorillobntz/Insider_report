import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import schedule
import time
import logging
import streamlit as st
import json

# Configuración de logging
logging.basicConfig(
    filename='insider_trading.log',  # Archivo donde se guardarán los logs
    level=logging.INFO,  # Nivel de logging
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato de los logs
)


api_key = st.secrets["Finnhub_API"]

# Autenticación con Google Sheets
def autenticar_google_sheets():
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    try:
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        logging.info("Credenciales de Google Sheets cargadas exitosamente.")
        cliente = gspread.authorize(creds)
        return cliente
    except Exception as e:
        logging.error(f"Error al cargar las credenciales de Google Sheets: {e}")
        return None

# Función para invertir el nombre de un directivo de Apellido Nombre -> Nombre Apellido
def invertir_nombre(nombre):
    partes = nombre.split()
    if len(partes) > 1:
        return ' '.join(partes[1:] + partes[:1])
    return nombre

# Función para obtener y limpiar las transacciones de insiders para un ticker
def obtener_transacciones_insiders(ticker):
    url = f'https://finnhub.io/api/v1/stock/insider-transactions?symbol={ticker}&token={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json().get('data', [])
        if not data:
            logging.info(f"No hay datos disponibles para {ticker}.")
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df = df[df['transactionCode'].isin(['P', 'S'])]
        columnas_necesarias = ['name', 'change', 'transactionPrice', 'share', 'transactionDate']
        df_limpio = df[columnas_necesarias]
        df_limpio.columns = ['Nombre', 'Cantidad', 'Precio de Transacción', 'Restantes', 'Fecha de Transacción']
        df_limpio['Nombre'] = df_limpio['Nombre'].apply(lambda x: invertir_nombre(x).title())
        df_limpio['Ticker'] = ticker
        logging.info(f"Transacciones de {ticker} obtenidas correctamente.")
        return df_limpio
    except json.JSONDecodeError:
        logging.error(f"Error al decodificar JSON para {ticker}.")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error desconocido para {ticker}: {e}")
        return pd.DataFrame()


def obtener_acciones_totales(ticker):
    accion = yf.Ticker(ticker)
    try:
        # Verifica si la clave 'sharesOutstanding' está en la info
        total_acciones = accion.info.get('sharesOutstanding', None)
        if total_acciones is None:
            logging.warning(f"No se encontró 'sharesOutstanding' para {ticker}. Valor predeterminado usado.")
            return 0  # Devuelve un valor predeterminado o maneja según sea necesario
        logging.info(f"Total de acciones obtenido para {ticker}: {total_acciones}")
        return total_acciones
    except json.JSONDecodeError:
        logging.error(f"Error de decodificación JSON para {ticker}.")
        return 0  # Valor predeterminado en caso de error
    except Exception as e:
        logging.error(f"Error desconocido al obtener el número total de acciones para {ticker}: {e}")
        return 0

# Función para crear resúmenes
def crear_resumen(df_compras, df_ventas):
    resumen_compras = df_compras.groupby('Ticker').agg({'Cantidad': 'sum', 'Precio de Transacción': 'mean'}).reset_index()
    resumen_compras['Precio de Transacción'] = resumen_compras['Precio de Transacción'].round(2)
    resumen_compras['Total Acciones'] = resumen_compras['Ticker'].apply(obtener_acciones_totales)
    resumen_compras['Porcentaje Comprado'] = resumen_compras.apply(
        lambda row: (row['Cantidad'] / row['Total Acciones'] * 100) if row['Total Acciones'] > 0 else 0,
        axis=1
    )
    resumen_compras.columns = ['Ticker', 'Total Comprado', 'Precio Medio Compra', 'Porcentaje Comprado', 'Total Acciones']
    resumen_compras = resumen_compras[['Ticker', 'Total Comprado', 'Precio Medio Compra', 'Porcentaje Comprado', 'Total Acciones']]
    
    resumen_ventas = df_ventas.groupby('Ticker').agg({'Cantidad': 'sum', 'Precio de Transacción': 'mean'}).reset_index()
    resumen_ventas['Precio de Transacción'] = resumen_ventas['Precio de Transacción'].round(2)
    resumen_ventas['Total Acciones'] = resumen_ventas['Ticker'].apply(obtener_acciones_totales)
    resumen_ventas['Porcentaje Vendido'] = resumen_ventas.apply(
        lambda row: (abs(row['Cantidad']) / row['Total Acciones'] * 100) if row['Total Acciones'] > 0 else 0,
        axis=1
    )
    resumen_ventas.columns = ['Ticker', 'Total Vendido', 'Precio Medio Venta', 'Porcentaje Vendido', 'Total Acciones']
    resumen_ventas = resumen_ventas[['Ticker', 'Total Vendido', 'Precio Medio Venta', 'Porcentaje Vendido', 'Total Acciones']]
    
    logging.info("Resúmenes de compras y ventas creados correctamente.")
    return resumen_compras, resumen_ventas

# Función para obtener transacciones de múltiples tickers
def obtener_transacciones_multiples_tickers(tickers):
    df_total = pd.DataFrame()
    for ticker in tickers:
        logging.info(f"Obteniendo datos para {ticker}...")
        df_ticker = obtener_transacciones_insiders(ticker)
        df_total = pd.concat([df_total, df_ticker], ignore_index=True)
    return df_total

# Función para dividir en compras y ventas
def dividir_compras_ventas(df):
    df_compras = df[df['Cantidad'] > 0]
    df_ventas = df[df['Cantidad'] < 0]
    logging.info("División de transacciones en compras y ventas completada.")
    return df_compras, df_ventas

# Función para filtrar transacciones por fecha
def filtrar_por_fecha(df, dias=15):
    fecha_limite = (datetime.now() - timedelta(days=dias)).date()
    df['Fecha de Transacción'] = pd.to_datetime(df['Fecha de Transacción']).dt.date
    df_filtrado = df[df['Fecha de Transacción'] >= fecha_limite]
    logging.info(f"Transacciones filtradas para los últimos {dias} días.")
    return df_filtrado

# Función para formatear la fecha a d/m/y
def formatear_fecha(df):
    df['Fecha de Transacción'] = pd.to_datetime(df['Fecha de Transacción'], errors='coerce').dt.date
    df = df.sort_values(by='Fecha de Transacción', ascending=False)
    df['Fecha de Transacción'] = df['Fecha de Transacción'].apply(lambda x: x.strftime('%d/%m/%Y') if pd.notnull(x) else '')
    logging.info("Fechas formateadas a d/m/y.")
    return df

# Guardar DataFrames en una hoja de Google Sheets
def guardar_en_google_sheets(df_compras, df_ventas, resumen_compras, resumen_ventas):
    cliente = autenticar_google_sheets()
    if cliente is None:
        logging.error("No se pudo autenticar con Google Sheets.")
        return

    try:
        sheet = cliente.open('Resumen Transacciones Insiders')
        worksheet_compras = sheet.worksheet('Compras')
        worksheet_ventas = sheet.worksheet('Ventas')
        worksheet_compras.clear()
        worksheet_ventas.clear()
        set_with_dataframe(worksheet_compras, df_compras)
        set_with_dataframe(worksheet_ventas, df_ventas)
        set_with_dataframe(worksheet_compras, resumen_compras, row=1, col=len(df_compras.columns) + 2)
        set_with_dataframe(worksheet_ventas, resumen_ventas, row=1, col=len(df_ventas.columns) + 2)
        logging.info("Datos guardados en Google Sheets correctamente.")
    except Exception as e:
        logging.error(f"Error al guardar en Google Sheets: {e}")

# Ejemplo de uso con varios tickers
tickers = ['ASML','ULTA','TXN','POOL','MSFT', 'MC','DHR','AAPL','SOM']

def automatizar_proceso(tickers):
    try:
        df_transacciones = obtener_transacciones_multiples_tickers(tickers)
        df_compras, df_ventas = dividir_compras_ventas(df_transacciones)
        df_compras = filtrar_por_fecha(df_compras)
        df_ventas = filtrar_por_fecha(df_ventas)
        df_compras = formatear_fecha(df_compras)
        df_ventas = formatear_fecha(df_ventas)
        resumen_compras, resumen_ventas = crear_resumen(df_compras, df_ventas)
        guardar_en_google_sheets(df_compras, df_ventas, resumen_compras, resumen_ventas)
        logging.info("Proceso de automatización completado exitosamente.")
    except Exception as e:
        logging.error(f"Error en el proceso de automatización: {e}")

automatizar_proceso(tickers)



