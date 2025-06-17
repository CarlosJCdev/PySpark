# 🧠 PySpark Data Engineering

Repositorio con scripts y notebooks utilizados para la gestión de datos utilizando PySpark y SQL en entornos distribuidos. Este repositorio incluye buenas prácticas, snippets reutilizables, y soluciones a problemas comunes al manejar grandes volúmenes de datos.

## 🚀 Funcionalidades y tareas comunes incluidas

### 🔄 Ingesta y transformación de datos
- 📥 Lectura de datos en formatos como Parquet, CSV, Delta, JSON desde ADLS o sistemas externos.
- 🔗 Unión y limpieza de datos provenientes de SAP
- 🧽 Normalización y validación de estructuras multi-tabla antes de persistencia.
- 🕹️ Automatización de procesos de ingestión por fecha, con widgets o parámetros externos

### 🧮 Procesamiento con PySpark
- 📊 Cálculo de agregaciones (por día, mes, país, etc.) usando `groupBy` y `agg`.
- 🔍 Comparación entre datasets (por ejemplo, diferencias UDL vs MDL).
- 🧬 Cálculo de métricas de calidad de datos (nulos, duplicados, desviaciones).
- 🧠 Uso de funciones personalizadas (`udf`, `withColumn`, `lit`, `when`).
- 🧰 Aplicación de filtros dinámicos y joins complejos con condiciones múltiples.

### 🧾 SQL sobre Spark
- 📝 Consultas SQL sobre DataFrames o tablas registradas en el metastore (`%sql` en Databricks).
- ⚖️ Comparación de datos con `EXCEPT`, `UNION ALL`, `JOIN` y `CASE WHEN`.
- 🚀 Optimización de queries con vistas temporales y persistencia intermedia.

### 🧰 Gestión de formatos y particiones
- 🗂️ Escritura de datos en tablas Delta particionadas (`partitionBy("COUNTRY", "FILE_DATE")`).
- 🔁 Migración de tablas Parquet a Delta Lake para mejorar consistencia.
- 🧾 Lectura de `_delta_log` y control de versiones.

### 🔁 Procesos programados y monitoreo
- ✅ Generación de validaciones diarias y mensuales de integridad entre capas.
- ⚠️ Detección automática de desviaciones de más del 5% en métricas clave.
- ⏱️ Control de ejecuciones por timestamp (`EXECUTION_TIMESTAMP`).
- 📧 Envío de alertas por correo vía Databricks Workflow o Azure Data Factory.

## 📦 Códigos y patrones populares incluidos

- 🔗 `unionByName` para combinar múltiples DataFrames.
- 🧩 `merge` tipo `MERGE INTO` para actualizaciones eficientes de tablas Delta.
- ❓ `isEmpty()` y `count()` como validación previa a escritura.
- 🧱 Uso de funciones `create_schema`, `truncate_table`, `get_latest_hour`.
- ⚙️ Modularización del código con funciones como:
  - `ret_mdl(granularity)`
  - `ret_udl(granularity)`
  - `reading_source_date(start, end)`
  - `loading_temp(df, path, table_type)`
