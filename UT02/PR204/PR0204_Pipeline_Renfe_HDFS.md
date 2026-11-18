# PR0204: Pipeline de Ingesta y Procesamiento en Data Lake con HDFS
## Contexto del Proyecto
Este cuaderno automatiza la creación de la arquitectura de un Data Lake, la ingesta de datos de estaciones y viajeros de Renfe en la capa cruda (*Raw*), y su posterior filtrado en flujo (*streaming*) hacia la capa procesada (*Processed*).


```python
import os
import csv
from hdfs import InsecureClient

# Definir conexión
client = InsecureClient('http://namenode:9870', user='root')

# ------------------ Apartado 3 ------------------ 
# FASE 1
# Creamos las carpetas una a una aplicando permisos POSIX como se pide
client.makedirs('/datalake/renfe/raw/estaciones', permission='755')
client.makedirs('/datalake/renfe/raw/viajeros', permission='755')
client.makedirs('/datalake/renfe/processed', permission='755')
client.makedirs('/datalake/renfe/logs', permission='755')

# Comprobamos que la estructura de directorios se ha creado correctamente
directorios = client.list('/datalake/renfe')
print(directorios)

# FASE 2
# Subida de los archivos de estaciones
client.upload('/datalake/renfe/raw/estaciones', 'estaciones_completo.csv', overwrite=True, n_threads=4)
client.upload('/datalake/renfe/raw/estaciones', 'estaciones_feve.csv', overwrite=True, n_threads=4)

# Subida del archivo de viajeros
client.upload('/datalake/renfe/raw/viajeros', 'viajeros_tramo.csv', overwrite=True, n_threads=4)

# Imp
for nombre, datos in client.list('/datalake/renfe/raw/estaciones', status=True):
    print("Nombre:", nombre, "| Longitud (bytes):", datos['length'], "| Permisos:", datos['permission'], "| Replicación:", datos['replication'])

# FASE 3
# Provincia que queremos filtrar
provincia = 'LEÓN'

# Definimos las rutas de origen y destino en HDFS
ruta_origen = '/datalake/renfe/raw/estaciones/estaciones_completo.csv'
ruta_destino = '/datalake/renfe/processed/estaciones_filtradas.csv'

# Índice de la columna donde se encuentra la provincia y contador de estaciones filtradas
indice_provincia = 7
contador = 0

# Abrimos el archivo original en modo lectura (streaming)
with client.read(ruta_origen, encoding='latin-1') as archivo_entrada:
    # Abrimos el archivo de destino en modo escritura
    with client.write(ruta_destino, overwrite=True, encoding='latin-1') as archivo_salida:
        # Leemos y conservamos la cabecera del CSV
        cabecera = archivo_entrada.readline()
        archivo_salida.write(cabecera)
        # Contador de estaciones filtradas
        contador = 0

        # Recorremos el archivo línea a línea
        for linea in archivo_entrada:
            # Ignoramos las líneas vacías
            if not linea.strip():
                continue
            
            # Separamos los campos de cada fila del CSV
            fila = next(csv.reader([linea], delimiter=';', quotechar='"'))

            # Comprobamos que la fila tiene suficientes columnas
            if len(fila) > indice_provincia:
                # Obtenemos el valor de la provincia y eliminamos espacios
                provincia_actual = fila[indice_provincia].strip().upper()

                # Comprobamos si coincide con la provincia seleccionada
                if provincia_actual == provincia.upper():
                    archivo_salida.write(linea)
                    contador += 1

# Mostramos el número de estaciones encontradas
print("Estaciones filtradas:", contador)

# Abrimos el archivo procesado en modo lectura (streaming)
with client.read(ruta_destino, encoding='latin-1') as archivo_muestra:
    # Leemos e imprimimos las primeras 5 líneas
    for _ in range(5):
        linea = archivo_muestra.readline()
        # Si llegamos al final del archivo, dejamos de leer
        if not linea:
            break
            
        # Imprimimos la línea eliminando saltos de línea y espacios sobrantes
        print(linea.strip())


# FASE 4
# Definimos la ruta del archivo de registro en HDFS
ruta_log = '/datalake/renfe/logs/ingesta.log'

# Creamos el archivo de registro y escribimos el primer evento
with client.write(ruta_log, overwrite=True, encoding='utf-8') as archivo_log:
    archivo_log.write('[INICIO] Pipeline ejecutado. Archivos crudos cargados en /raw\n')

# Añadimos el segundo evento al archivo de registro sin borrar el anterior
with client.write(ruta_log, append=True, encoding='utf-8') as archivo_log:
    archivo_log.write(f'[PROCESADO] Se han filtrado un total de {contador} estaciones para la provincia seleccionada.\n')

# Leemos y mostramos el contenido completo del archivo de registro
print("\n--- CONTENIDO DEL ARCHIVO DE LOG ---")

with client.read(ruta_log, encoding='utf-8') as archivo_log:
    contenido = archivo_log.read()
    print(contenido)


# FASE 5
# Definimos la ruta local donde guardaremos el archivo descargado
ruta_local = 'entrega_estaciones.csv'

# Descargamos el fichero procesado habilitando la sobrescritura
client.download('/datalake/renfe/processed/estaciones_filtradas.csv', ruta_local, overwrite=True)

# Mostramos un mensaje confirmando la descarga
print("Archivo procesado descargado correctamente:", ruta_local)

# Eliminamos únicamente el archivo de estaciones FEVE de HDFS
# Definimos la ruta del archivo que queremos eliminar
ruta_feve = '/datalake/renfe/raw/estaciones/estaciones_feve.csv'

# Eliminamos el archivo de FEVE
client.delete(ruta_feve)

# Mostramos el contenido de la carpeta para comprobar que el archivo
# estaciones_completo.csv permanece intacto
print("\nContenido de la carpeta raw/estaciones después de eliminar FEVE:")

archivos_estaciones = client.list('/datalake/renfe/raw/estaciones')

for archivo in archivos_estaciones:
    print(archivo)

#Definimos una función para reiniciar todo el Data Lake
def reset_datalake():
    # Eliminamos toda la estructura del proyecto de forma recursiva
    client.delete('/datalake/renfe', recursive=True)
    print("Data Lake eliminado correctamente.")


# IMPORTANTE:
# Dejamos la función definida, pero no la ejecutamos.
# Si la ejecutamos, eliminaremos toda la estructura /datalake/renfe.

```

    ['logs', 'processed', 'raw']
    Nombre: estaciones_completo.csv | Longitud (bytes): 142155 | Permisos: 644 | Replicación: 3
    Nombre: estaciones_feve.csv | Longitud (bytes): 61795 | Permisos: 644 | Replicación: 3
    Estaciones filtradas: 43
    "CODIGO";"DESCRIPCION";"LATITUD";"LONGITUD";"DIRECION";"CP";"POBLACION";"PROVINCIA";"PAIS";"CERCANIAS";"ANCHO MÉTRICO";"COMUN"
    "05755";"LA LLAMA DE LA GUZPEÑA";"42.7702481";"-5.0412478";"Ctra. Cl - 626 --- Estación Ferrocarril, S/N";"24893";"PRADO DE LA GUZPEÑA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05756";"LA ESPINA";"42.7868560";"-4.9052025";"Ctra. Cl - 626 --- Estación Ferrocarril, S/N";"24889";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05757";"VALCUENDE";"42.7655780";"-4.9292558";"C/ Arriba, S/N";"24889";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05758";"PUENTE ALMUHEY";"42.7876320";"-4.9718992";"Ctra. General  S/N- Puente Almuhey";"24880";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""
    
    --- CONTENIDO DEL ARCHIVO DE LOG ---
    [INICIO] Pipeline ejecutado. Archivos crudos cargados en /raw
    [PROCESADO] Se han filtrado un total de 43 estaciones para la provincia seleccionada.
    
    Archivo procesado descargado correctamente: entrega_estaciones.csv
    
    Contenido de la carpeta raw/estaciones después de eliminar FEVE:
    estaciones_completo.csv



```python

```
