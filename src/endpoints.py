from flask import Flask, jsonify, request
import psycopg
import os
from dotenv import load_dotenv


app = Flask(__name__)
tabla = "variables"


def conectar():
    
    load_dotenv()        
    conn = psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return conn


@app.route('/')
def saludo():
    """Ruta de saludo inicial."""
    return jsonify({'hola': 'mundo'})

    
@app.route('/variables')
def fetch_variables():
    """Obtiene las variables de la base de datos."""
    with conectar() as conn:
        with conn.cursor() as curs:
            query = f"""
                WITH aux AS (
                    SELECT id, 
                            CONCAT(name, '_-_', bin) AS name, 
                            ARRAY_REMOVE(
                                ARRAY[
                                CASE WHEN interval_state IS NOT NULL AND cells_state IS NOT NULL AND cells_state <> '{{}}' THEN 'state' END,
                                CASE WHEN interval_mun IS NOT NULL AND cells_mun IS NOT NULL AND cells_mun <> '{{}}' THEN 'mun' END,
                                CASE WHEN interval_ageb IS NOT NULL AND cells_ageb IS NOT NULL AND cells_ageb <> '{{}}' THEN 'ageb' END
                                ]::varchar[], 
                                NULL
                            ) AS available_grids, 
                            0 AS level_size, 
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
    grid_id = request.args.get('grid_id')  # mun | state | ageb
    levels_id = request.args.get('levels_id', type=lambda v: v.split(','))
    filter_names = request.args.get('filter_names', type=lambda v: v.split(','))
    filter_values = request.args.get('filter_values', type=lambda v: v.split(','))

    if not grid_id:
        return jsonify({'error': 'grid_id es requerido'}), 400

    with conectar() as conn:
        with conn.cursor() as curs:
            query = f"""
                WITH aux AS (
                    SELECT id, 
                            %s as grid_id, 
                            0 as level_id, 
                            cells_{grid_id} :: text[] AS cells, 
                            array_length(string_to_array(cells_{grid_id}, ','), 1) AS n
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
    app.run(host='0.0.0.0', port=2112)