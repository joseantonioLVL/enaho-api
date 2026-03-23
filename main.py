from flask import Flask, request, jsonify
import sqlite3
import os
from collections import defaultdict
import gc

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), 'enaho.db')

STOPWORDS = {'que', 'como', 'para', 'una', 'uno', 'los', 'las', 'del', 'con', 'por', 'hay', 'sobre', 'existe', 'variable', 'variables', 'datos', 'dato', 'enaho', 'quiero', 'busco', 'necesito', 'encontrar', 'tesis', 'investigacion'}

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def buscar_en_db(palabras, limit=150):
    conn = get_db()
    try:
        cur = conn.cursor()
        condiciones = []
        params = []
        for p in palabras:
            condiciones.append("(variable_lower LIKE ? OR label LIKE ?)")
            params.extend([f'%{p}%', f'%{p}%'])
        
        query = f"SELECT variable, variable_lower, label, modulo_codigo, modulo_nombre, archivo, anio, n_obs, tipo FROM variables WHERE {' OR '.join(condiciones)} LIMIT {limit}"
        cur.execute(query, params)
        resultados = [dict(row) for row in cur.fetchall()]
        return resultados
    finally:
        conn.close()
        gc.collect()

def agrupar_por_variable(resultados):
    grupos = defaultdict(lambda: {
        'variable': '',
        'label': '',
        'modulo_codigo': '',
        'modulo_nombre': '',
        'archivos': set(),
        'anos': [],
        'tipo': '',
        'obs_por_ano': {}
    })
    for v in resultados:
        key = v['variable_lower']
        g = grupos[key]
        g['variable'] = v['variable']
        g['label'] = v['label']
        g['modulo_codigo'] = v['modulo_codigo']
        g['modulo_nombre'] = v['modulo_nombre']
        g['archivos'].add(v['archivo'])
        g['anos'].append(v['anio'])
        g['tipo'] = v['tipo']
        g['obs_por_ano'][v['anio']] = v['n_obs']
    return grupos

@app.route('/buscar', methods=['GET'])
def buscar():
    pregunta = request.args.get('q', '').lower()
    palabras = [p for p in pregunta.replace(',', ' ').split()
                if len(p) > 3 and p not in STOPWORDS]

    if not palabras:
        return jsonify({'total': 0, 'contexto': 'No se encontraron palabras clave.', 'pregunta': pregunta})

    resultados = buscar_en_db(palabras)
    grupos = agrupar_por_variable(resultados)
    top = list(grupos.values())[:30]

    lineas = []
    for g in top:
        anos_ordenados = sorted(set(g['anos']))
        obs_sample = list(g['obs_por_ano'].items())[-1]
        archivos = ', '.join(sorted(g['archivos']))
        lineas.append(
            f"- Variable: {g['variable']} | "
            f"Label: {g['label']} | "
            f"Módulo: {g['modulo_nombre']} ({g['modulo_codigo']}) | "
            f"Archivos: {archivos} | "
            f"Años disponibles: {anos_ordenados} | "
            f"Obs ({obs_sample[0]}): {obs_sample[1]} | "
            f"Tipo: {g['tipo']}"
        )

    gc.collect()
    contexto = '\n'.join(lineas)
    return jsonify({
        'total': len(top),
        'contexto': f"Encontré {len(top)} variables únicas relevantes en la ENAHO:\n\n{contexto}" if top else 'No encontré variables que coincidan.',
        'pregunta': request.args.get('q', '')
    })

@app.route('/health', methods=['GET'])
def health():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM variables')
        count = cur.fetchone()[0]
        return jsonify({'status': 'ok', 'registros': count})
    finally:
        conn.close()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
