import json
from collections import OrderedDict

def rename_duplicate_keys(item):
    """
    Renombra el campo duplicado 'dsSegmentoMkt' a 'dsSegmentoMkt_alt'
    """
    seen_keys = set()
    new_item = OrderedDict()

    for key, value in item.items():
        if key == 'dsSegmentoMkt' and key in seen_keys:
            new_key = 'dsSegmentoMkt_alt'
            new_item[new_key] = value
        else:
            new_item[key] = value
            seen_keys.add(key)
    
    return new_item

def preprocess_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file, object_pairs_hook=OrderedDict)
    
    # Preprocesar los elementos en el array 'VentasResumen'
    for i, item in enumerate(data['dsReporteComprobantesApi']['VentasResumen']):
        data['dsReporteComprobantesApi']['VentasResumen'][i] = rename_duplicate_keys(item)
    
    return data

def save_preprocessed_json(data, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

# Uso
input_file_path = 'ventassindetalle.json'  # Ruta del archivo JSON original
output_file_path = 'ventassindetalle_procesado.json'  # Ruta del archivo JSON preprocesado

# Preprocesar el archivo JSON y guardar el resultado
data = preprocess_json(input_file_path)
save_preprocessed_json(data, output_file_path)

