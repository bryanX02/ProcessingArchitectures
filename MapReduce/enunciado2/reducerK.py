#!/usr/bin/python3

import sys

top_urls = []

# Leemos la entrada desde stdin
for line in sys.stdin:
    key, value = line.strip().split('\t')
    value = int(value)
    
    # Insertar en la lista
    top_urls.append((key, value))

# Ordenamos por número de accesos en orden descendente
top_urls.sort(key=lambda x: x[1], reverse=True)

# E imprimimos las 20 URLs más accesadas
for key, value in top_urls[:20]:
    print(f"{key}\t{value}")

'''
mapper.py extrae las URLs y emite url\t1.
reducer.py suma las ocurrencias de cada URL.
mapperK.py filtra y ordena las 20 más altas.
reducerK.py asegura que los 20 valores finales sean los correctos.
'''