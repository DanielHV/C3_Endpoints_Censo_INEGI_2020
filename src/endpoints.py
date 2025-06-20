from flask import Flask, jsonify, request
import psycopg
import os
from dotenv import load_dotenv
import requests
import argparse
import json

col_info = None


app = Flask(__name__)
tabla = os.getenv("DB_TABLE", "variables")


def conectar():
    """Establece una conexión a la base de datos PostgreSQL usando credenciales cargadas desde variable de entorno"""
    load_dotenv()        
    conn = psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn


def obtener_resolution_por_grid_id(grid_id):
    """Consulta el endpoint dado y devuelve el valor de 'resolution' para el grid_id especificado"""
    resp = requests.get("http://chilamdev.c3.unam.mx:5001/regions/region-grids/")
    resp.raise_for_status()
    data = resp.json()
    for item in data.get("data", []):
        if str(item.get("grid_id")) == str(grid_id):
            return item.get("resolution")
    return None


@app.route('/')
def saludo():
    """Ruta de saludo inicial."""
    return jsonify({'hola': 'mundo'})

    
@app.route('/variables')
def fetch_variables():
    """Obtiene las variables de la base de datos."""
    with conectar() as conn:
        with conn.cursor() as curs:
            
            levels = col_info["levels"]
            name = "ARRAY[" + ", ".join(levels) + "]::varchar[]"
            
            casos = []
            for grid, info in col_info.get("grids", {}).items():
                cond = f"{info["data"]} IS NOT NULL OR {info["data"]} <> '{{}}'"
                caso = f"CASE WHEN {cond} THEN '{grid}' END"
                casos.append(caso)
            available_grids = "ARRAY_REMOVE(ARRAY[" + ", ".join(casos) + "]::varchar[], NULL)"

            query = f"""
                WITH aux AS (
                    SELECT id, 
                        {name} AS name, 
                        {available_grids} AS available_grids, 
                        {len(levels)} AS level_size, 
                        ARRAY[]::varchar[] AS filter_fields
                    FROM {tabla}
                )
                SELECT json_agg(aux) FROM aux;
            """
            curs.execute(query)
            row = curs.fetchone()  # Devuelve una tupla
    return jsonify(row[0])


@app.route('/variables/<id>')
def variables_id(id):
    """Obtiene una variable específica por su ID."""
    q = request.args.get('q', '*')
    offset = request.args.get('offset', '0')  # Cambiado a 0 por defecto
    limit = request.args.get('limit', 10)

    with conectar() as conn:
        with conn.cursor() as curs:
            query = f"""
                SELECT id, 0 as level_id 
                FROM {tabla} 
                WHERE id = %s 
                LIMIT %s OFFSET %s;
            """
            curs.execute(query, (id, limit, offset))
            r = curs.fetchone()

            if not r:
                return jsonify({'error': 'ID no encontrado'}), 404

            cols = [desc[0] for desc in curs.description]  # Nombres de las columnas
            result = {columna: r[i] for i, columna in enumerate(cols)}
            
    return jsonify(result)


@app.route('/get-data/<id>')
def get_data_id(id):
    """Obtiene datos específicos de una covariable por ID y filtros opcionales."""
    grid_id = request.args.get('grid_id')  # state:17 | mun:18 | ageb:19
    levels_id = request.args.get('levels_id', type=lambda v: v.split(','))
    filter_names = request.args.get('filter_names', type=lambda v: v.split(','))
    filter_values = request.args.get('filter_values', type=lambda v: v.split(','))

    if not grid_id:
        return jsonify({'error': 'grid_id es requerido'}), 400
    
    grid = obtener_resolution_por_grid_id(grid_id)
    if not grid:
        return jsonify({'error': 'ID no encontrado'}), 404
    
    col_data = col_info["grids"][grid]["data"]

    with conectar() as conn:
        with conn.cursor() as curs:
            query = f"""
                WITH aux AS (
                    SELECT id, 
                            %s as grid_id, 
                            0 as level_id, 
                            {col_data} :: text[] AS cells, 
                            array_length(string_to_array({col_data}, ','), 1) AS n
                    FROM variables
                    WHERE id = %s
                )
                SELECT json_agg(aux) FROM aux;
            """
            curs.execute(query, (grid_id, id))
            row = curs.fetchone()

    if not row:
        return jsonify({'error': 'ID no encontrado'}), 404

    return jsonify(row[0])


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Endpoints Censo INEGI 2020")
    parser.add_argument('--column-info', type=str, required=True, help='Ruta al archivo JSON que indica la información correspondiente a las columnas')
    args = parser.parse_args()

    with open(args.column_info) as f:
        col_info = json.load(f)

    with conectar() as conn:
        with conn.cursor() as curs:
            curs.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (tabla,))
            columnas_validas = {row[0] for row in curs.fetchall()}

    columnas_json = set()
    
    if "levels" not in col_info:
        raise ValueError(f"El archivo JSON debe tener la clave 'levels'")
    levels = col_info.get("levels", [])
    if not isinstance(levels, list) or not all(isinstance(x, str) for x in levels):
        raise ValueError("El valor asociado a 'levels' debe ser una lista de cadenas no vacia")
    columnas_json.update(levels)
    
    if "grids" not in col_info:
        raise ValueError(f"El archivo JSON debe tener la clave 'grids'")
    grids = col_info.get("grids", {})
    if not isinstance(grids, dict) or not bool(grids):
        raise ValueError("'grids' debe ser un diccionario no vacio")
    for grid_name, grid_info in grids.items():
        if not isinstance(grid_info, dict):
            raise ValueError(f"El valor asociado a '{grid_name}' debe ser un diccionario")
        if "data" not in grid_info:
            raise ValueError(f"El grid '{grid_name}' debe tener la clave 'data'")
        data = grid_info.get("data")
        if not isinstance(data, str):
            raise ValueError("El valor asociado a 'data' debe ser una cadena")
        columnas_json.add(data)

    columnas_invalidas = columnas_json - columnas_validas
    if columnas_invalidas:
        raise ValueError(f"Las siguientes columnas no existen: {columnas_invalidas}")

    app.run(host='0.0.0.0', port=2112)