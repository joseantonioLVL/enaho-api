from flask import Flask, request, jsonify
import json
from collections import defaultdict
import os
import requests

app = Flask(__name__)

STOPWORDS = ['que', 'como', 'para', 'una', 'uno', 'los', 'las', 'del', 'con', 'por', 'hay', 'sobre', 'existe', 'variable', 'variables', 'datos', 'dato', 'enaho', 'quiero', 'busco', 'necesito', 'encontrar', 'tesis', 'investigacion']

# Cargar metadata desde Google Drive al iniciar
def cargar_metadata():
    url = "https://drive.google.com/uc?export=download&id=1dCfMXjt_EG9h5GWSIazQ8QpOnbvqfLH7&confirm=t"
    print("Descargando metadata desde Drive...")
    r = requests.get(url, timeout=60)
    data = r.json()
    print(f"Metadata cargada: {len(data)} registros")
    return data

metadata = cargar_metadata()

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
    
    resultados = [v for v in metadata if any(
        p in f"{v['variable']} {v['variable_lower']} {v['label']}".lower() 
        for p in palabras
    )]
    
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
    
    contexto = '\n'.join(lineas)
    
    return jsonify({
        'total': len(top),
        'contexto': f"Encontré {len(top)} variables únicas relevantes en la ENAHO:\n\n{contexto}" if top else 'No encontré variables que coincidan.',
        'pregunta': request.args.get('q', '')
    })

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'registros': len(metadata)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
