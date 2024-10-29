import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import logging

# Configuración de logging
logging.basicConfig(
    filename='insider_trading.log',  # Archivo donde se guardarán los logs
    level=logging.INFO,  # Nivel de logging
    format='%(asctime)s - %(levelname)s - %(message)s'  # Formato de los logs
)

# Cargar variables de entorno desde .env
load_dotenv()
api_key = os.getenv('Finnhub_API')

# Autenticación con Google Sheets
def autenticar_google_sheets():
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name('Credenciales_API.json', scope)
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
    except requests.exceptions.RequestException as e:
        logging.error(f"Error de red al obtener datos para {ticker}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error desconocido para {ticker}: {e}")
        return pd.DataFrame()


# Función para obtener acciones totales para múltiples tickers
def obtener_acciones_totales(tickers):
    resultados = {}
    for ticker in tickers:
        accion = yf.Ticker(ticker)
        try:
            # 1. Intenta obtener `sharesOutstanding`, que es el más confiable.
            total_acciones = accion.info.get('sharesOutstanding')
            if total_acciones is not None:
                logging.info(f"Total de acciones ('sharesOutstanding') obtenido para {ticker}: {total_acciones}")
                resultados[ticker] = total_acciones
                continue
            
            # 2. Si `sharesOutstanding` es None, intenta con `totalSharesOutstanding`.
            total_acciones = accion.info.get('totalSharesOutstanding')
            if total_acciones is not None:
                logging.info(f"Total de acciones ('totalSharesOutstanding') obtenido para {ticker}: {total_acciones}")
                resultados[ticker] = total_acciones
                continue
            
            # 3. Si `totalSharesOutstanding` también es None, intenta con `floatShares`.
            total_acciones = accion.info.get('floatShares')
            if total_acciones is not None:
                logging.info(f"Total de acciones ('floatShares') obtenido para {ticker}: {total_acciones}")
                resultados[ticker] = total_acciones
                continue
            
            # 4. Si `floatShares` es None, calcula estimación usando `marketCap` / `currentPrice`.
            market_cap = accion.info.get('marketCap')
            current_price = accion.info.get('currentPrice')
            if market_cap is not None and current_price is not None:
                total_acciones_calculado = market_cap / current_price
                logging.info(f"Estimación de acciones calculada para {ticker} usando 'marketCap' y 'currentPrice': {total_acciones_calculado}")
                resultados[ticker] = int(total_acciones_calculado)
                continue

            # Si ninguna de las opciones devuelve un valor, lanza una advertencia y retorna 0.
            logging.warning(f"No se encontró información sobre el total de acciones para {ticker}. Valor predeterminado usado.")
            resultados[ticker] = 0
            
        except Exception as e:
            logging.error(f"Error desconocido al obtener el número total de acciones para {ticker}: {e}")
            resultados[ticker] = f"Error: {str(e)}"

    return resultados

# Función para crear resúmenes de compras y ventas
def crear_resumen(df_compras, df_ventas, total_acciones):
    def calcular_porcentaje(cantidad, total_acciones):
        # Calculo el porcentaje en relación al total de acciones
        return ((abs(cantidad) / total_acciones)) * 100 if total_acciones > 0 else 0

    def procesar_resumen(df, tipo):
        # Agrupo por ticker y calculo el total y el precio medio de transacción
        resumen = df.groupby('Ticker').agg({'Cantidad': 'sum', 'Precio de Transacción': 'mean'}).reset_index()
        resumen['Precio de Transacción'] = resumen['Precio de Transacción'].round(2)
        
        # Asignar total de acciones desde el diccionario
        resumen['Total Acciones'] = resumen['Ticker'].apply(lambda x: total_acciones.get(x, 0))

        # Calculo el porcentaje en base al total de acciones para cada ticker
        resumen[f'Porcentaje {tipo}'] = resumen.apply(
            lambda row: calcular_porcentaje(row['Cantidad'], row['Total Acciones']),
            axis=1
        )
    
        # Redondeo los valores del porcentaje para mayor claridad
        resumen[f'Porcentaje {tipo}'] = resumen[f'Porcentaje {tipo}'].round(5)
        
        # Renombro las columnas según el tipo (Comprado o Vendido) y ajusto el orden
        resumen.columns = ['Ticker', f'Total {tipo}', f'Precio Medio {tipo}', 'Total Acciones', f'Porcentaje {tipo}']

        # Retorno solo las columnas deseadas en el orden correcto (porcentaje primero)
        return resumen[['Ticker', f'Total {tipo}', f'Precio Medio {tipo}', f'Porcentaje {tipo}', 'Total Acciones']]

    # Creo los resúmenes de compras y ventas aplicando la función procesada
    resumen_compras = procesar_resumen(df_compras, 'Comprado')
    resumen_ventas = procesar_resumen(df_ventas, 'Vendido')

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
days = 29
def filtrar_por_fecha(df, dias=days):
    fecha_limite = (datetime.now() - timedelta(days=dias)).date()
    df['Fecha de Transacción'] = pd.to_datetime(df['Fecha de Transacción'], errors='coerce').dt.date
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
tickers = ['ASML','ULTA','TXN','POOL','MSFT', 'MC','DHR','AAPL','SOM','NVDA','AAPL','GOOGL']

def automatizar_proceso(tickers):
    try:
        df_transacciones = obtener_transacciones_multiples_tickers(tickers)
        df_compras, df_ventas = dividir_compras_ventas(df_transacciones)
        df_compras = filtrar_por_fecha(df_compras)
        df_ventas = filtrar_por_fecha(df_ventas)
        df_compras = formatear_fecha(df_compras)
        df_ventas = formatear_fecha(df_ventas)
        total_acciones = obtener_acciones_totales(tickers)
        resumen_compras, resumen_ventas = crear_resumen(df_compras, df_ventas, total_acciones)

        guardar_en_google_sheets(df_compras, df_ventas, resumen_compras, resumen_ventas)
        logging.info("Proceso de automatización completado exitosamente.")
    except Exception as e:
        logging.error(f"Error en el proceso de automatización: {e}")

# Ejecutar el proceso
automatizar_proceso(tickers)

