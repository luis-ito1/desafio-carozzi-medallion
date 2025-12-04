# ---------------------------------------------- #
  Desafío Data Engineer – Arquitectura Medallion
# ---------------------------------------------- #

1. Contexto y objetivo del desafío

Este proyecto tiene como propósito construir un flujo completo de ingeniería de datos utilizando Python, PySpark y Docker, siguiendo la arquitectura Medallion (Bronze → Silver → Gold).

La empresa entrega varios archivos CSV con información de:

* Pedidos (cabecera y detalle)
* Clientes
* Productos

Con estos datos se debe preparar una tabla final de features por cliente, enfocada en identificar comportamiento reciente y medir actividad comercial.

Uno de los requisitos clave es identificar a los clientes activos del canal tradicional. Para efectos del negocio, un cliente se considera activo si:

    - Ha realizado al menos un pedido en los últimos 3 meses
    - y pertenece al canal tradicional.

A partir de esto se construyen distintas métricas que alimentarán análisis posteriores o modelos predictivos.


2. Arquitectura Medallion aplicada

El proyecto replica de manera simple el clásico flujo Medallion:

# --------------------------------------------------------- #
                Bronze – Ingesta inicial
# --------------------------------------------------------- #

Aquí solo se cargan los CSV tal cual vienen desde data/raw.
En esta etapa:

* Se leen los CSV.
* Se agregan columnas de metadata (ingestion_timestamp, source_file).
* Se guardan en formato Parquet una carpeta por tabla.

La idea es conservar siempre una copia confiable y sin alterar del origen, pero en un formato más eficiente.

# --------------------------------------------------------- #
            Silver – Limpieza y estandarización
# --------------------------------------------------------- #

En esta capa se hace el trabajo de “poner en orden” los datos:

* Cast de tipos (fechas, enteros, decimales).
* Eliminación de registros no válidos (por ejemplo, ventas negativas).
* Eliminación de duplicados en claves importantes.
* Creación de tablas listas para análisis:
    - fact_order_line: unión de cabecera + detalle a nivel línea.
    - dim_customer
    - dim_product
    - dim_date (generada entre el mínimo y máximo de fechas del fact).

Esta capa es la base para cualquier análisis posterior.

# --------------------------------------------------------- #
            Gold – Cálculo de métricas de negocio
# --------------------------------------------------------- #

En esta etapa se genera la tabla final:
    dim_features

Aquí se hace lo siguiente:

    * Se consideran solo clientes del canal tradicional.
    * Se calculan métricas basadas exclusivamente en los últimos 3 meses.
    * Se construyen features que caracterizan el comportamiento del cliente.

La salida final se guarda en data/gold/dim_features.

3. Diccionario de datos de dim_features

    Feature	                       |    Qué significa
    ventas_total_3m	               |    Total comprado por el cliente en los últimos 3 meses.
    frecuencia_pedidos_3m          |    Número de pedidos realizados en ese periodo.
    ticket_promedio_3m	           |    Monto promedio por pedido.
    porcentaje_pedidos_promo_3m	   |    % de pedidos que tuvieron promoción.
    recencia_dias	               |    Días desde la última compra del cliente hasta la fecha de referencia.
    dias_promedio_entre_pedidos	   |    Tiempo promedio entre compras consecutivas.
    variedad_categorias_3m	       |    Cantidad de categorías distintas que compró.

Esta tabla es la que se usará para análisis, segmentaciones y/o modelos ML.

4. Estructura del proyecto

Desafio-Carozzi/
 ├─ Dockerfile
 ├─ main.py
 ├─ src/
 │   ├─ bronze/
 │   │   └─ bronze_ingest.py
 │   ├─ silver/
 │   │   └─ silver_transform.py
 │   └─ gold/
 │       └─ gold_features.py
 └─ data/
     ├─ raw/
     ├─ bronze/
     ├─ silver/
     └─ gold/

La carpeta raw/ debe contener los CSV iniciales.
Las demás carpetas se generan automáticamente al correr el pipeline.

5. Cómo ejecutar el proyecto

    5. 1. Construir la imagen Docker

        Desde la carpeta raíz:
        docker build -t carozzi-medallion .

    5. 2. Ejecutar el pipeline completo

        docker run --rm -it -v C:\Users\DELL\Desktop\Desafio-Carozzi\data:/app/data carozzi-medallion

        Esto ejecuta en orden:
            a. Ingesta Bronze
            b. Transformación Silver
            c. Generación de la tabla Gold
