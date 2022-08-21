#!/usr/bin/env python3

import requests
import pandas as pd

"""
La idea es consolidar la información de `bolivia-maes.json` en una sola tabla fácil de consultar, monitorear y extender.
"""

URL = 'https://raw.githubusercontent.com/BoliviaMaes/bolivia-maes/main/bolivia-maes.json' # la dirección de los datos
INDEX_COLUMNA = 'airtableId' # la columna para hacer join en las tablas de entidades y personas

# las columnas en cada tabla que pienso consumir
ENTIDADES_COLUMNAS = ['dependencia', 'eleccion_mae', 'nombre', 'tipo', 'sigla', 'twitter', 'webpage', 'desde', 'fuente_inicio', 'hasta', 'sucesoras']
PERSONAS_COLUMNAS = ['genero', 'nombre', 'twitter']
AUTORIDADES_COLUMNAS = ['entidad', 'persona', 'cargo', 'desde', 'fuente_inicio', 'tweet_inicio', 'causa_fin', 'hasta', 'sucesora', 'tweet_fin', 'fuente_fin']

def prepare_table(dataframe, fill_columns, selected_columns, prefix, index_name):
    """
    Preparaciones para `entidades` y `personas`.
    dataframe: la tabla
    fill_columns: columnas con relaciones representadas en airtable IDs que deberían ser reemplazadas por nombres legibles
    selected_columns: columnas que nos interesa consultar
    prefix: texto de prefijo para el nombre de columnas a manera de evitar colisiones
    index_name: cómo se llama la columna que representa relaciones a esta tabla en `autoridades`
    """
    
    def fill(dataframe, columna_index, columna_name):
        # Reemplaza columnas de relaciones representadas como airtable IDs por nombres legibles
        nombres = dataframe.set_index('airtableId')[columna_name].to_dict()
        return dataframe[columna_index].apply(lambda d: nombres[d[0]] if type(d) == list else None) 
    
    for column in fill_columns:
        dataframe[column] = fill(dataframe, column, 'nombre')
    columns = [index_name] + selected_columns
    dataframe = dataframe.rename(columns={'airtableId': index_name})[columns]
    dataframe.columns = [index_name] + ['{}_{}'.format(prefix, column) for column in selected_columns]
    return dataframe

maes = requests.get(URL).json() # descarga los datos 
autoridades, entidades, personas = [pd.json_normalize(maes[k]) for k in ['autoridades', 'entidades', 'personas']] # extrae las tablas

# prepara `entidades`
entidades = prepare_table(
    entidades, 
    ['dependencia', 'sucesoras'], 
    ENTIDADES_COLUMNAS,
    'entidad',
    'entidad')

# prepara `personas`
personas = prepare_table(
    personas,
    [],
    PERSONAS_COLUMNAS,
    'persona',
    'persona')

# prepara `autoridades`
autoridades = autoridades[AUTORIDADES_COLUMNAS]
autoridades.entidad = autoridades.entidad.apply(lambda c: c[0])
autoridades.persona = autoridades.persona.apply(lambda c: c[0])

# enriquece `autoridades` con información de las otras tablas
autoridades = pd.merge(autoridades, personas, on='persona', how='outer')
autoridades = pd.merge(autoridades, entidades, on='entidad', how='outer')
autoridades.drop(columns=['entidad', 'persona'], inplace=True) # se deshace de columnas con IDs
autoridades = autoridades[autoridades.notna().sum(axis=1) > 0] # remueve filas donde sólo hay NaNs

autoridades.sort_values('desde').to_csv('data/maes.csv', index=False) # guarda la tabla consolidada en orden cronológico de asignación