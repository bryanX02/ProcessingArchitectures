#!/usr/bin/python3

import sys

# Lista para almacenar las URLs y sus frecuencias
top_urls = []

# Leer entrada desde stdin
for line in sys.stdin:
    key, value = line.strip().split('\t')
    value = int(value)
    
    # Insertar ordenadamente en la lista
    top_urls.append((key, value))
    top_urls.sort(key=lambda x: x[1], reverse=True)
    
    # Mantener solo las 20 entradas más grandes
    if len(top_urls) > 20:
        top_urls.pop()

# Imprimir las 20 URLs más frecuentes
for key, value in top_urls:
    print(f"{key}\t{value}")


