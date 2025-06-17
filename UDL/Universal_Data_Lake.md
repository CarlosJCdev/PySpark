# Universal Data Lake (UDL)

Un lago de datos universal es un concepto donde los datos se ingieren una sola vez y se consumen directamente desde el lago de datos sin reingestas. 
En este enfoque, diversas bases de datos como Spark, Athena, Presto, etc., se utilizan principalmente como marcos de ejecución que pueden reutilizar los datos en el lago de datos, 
así como conservarlos para que un marco diferente los utilice de forma portátil. Algunos de los objetivos técnicos podrían incluir.


Portabilidad de datos
 ---------------
* minimice el movimiento de datos fuera del lago y reduzca la cantidad de copias de datos en varios entornos de ejecución.

Reutilización de datos
---------------
* Almacenar datos en un formato de código abierto que se traduzca entre diversos marcos de ejecución. 
* Idealmente, los consumidores deberían poder intercambiar tramas de datos en memoria entre marcos de ejecución.

Almacenamiento de datos de acceso global
  ---------------
  * almacene datos en ubicaciones de almacenamiento que sean accesibles globalmente y al mismo tiempo proporcione una infraestructura RBAC, lo suficientemente robusta para soportar un control de acceso granular.
  
Metadatos accesibles globalmente
---------------
  * Asegúrese de que los metadatos sean globales y accesibles en diferentes marcos.

* * *

Para cumplir con los requisitos anteriores de un lago de datos universal, el lago de datos debe tener las siguientes características:
Datos de fácil acceso: Esto implica que los datos deben almacenarse en un sistema de almacenamiento al que varias instancias puedan acceder simultáneamente. Los almacenes de objetos como S3 o Azure Blob Storage ofrecen una excelente solución de almacenamiento, ya que son asequibles y accesibles para un gran número de instancias simultáneamente sin necesidad de un proceso de montaje.

Un formato de datos universal: los datos almacenados en formatos de columnas de código abierto, como Parquet o Delta Lake, son universalmente accesibles para la mayoría de los marcos de ejecución.

Un metastore externo: el metastore de Hive en MySQL/Postgres es accesible en modo de lectura y escritura para todos los marcos de ejecución que proporcionan acceso al esquema global.

![image](https://github.com/user-attachments/assets/4c053774-a5a3-4998-a3b5-c27554851eb2)


## Escenario de una implementación de BIG DATA con UDL:

Como antecedente, en BFA, utilizamos una arquitectura basada en microservicios en AWS para gestionar nuestros datos de producción. Los datos se ingieren desde las bases de datos de producción mediante DMS e infraestructura de eventos al lago de datos, alojado en S3. Utilizamos Spark para preprocesar y extraer datos (ETL) de los datos, creando así la versión final en Parquet. El Metastore está alojado en RDS con Hive+MySQL. Dado que nuestros equipos de análisis, aprendizaje automático y diversos equipos de negocio consumen datos del lago de datos, el acceso basado en roles de IAM ayuda a proteger los datos y a proporcionar acceso aislado a los equipos relevantes.

Dado que toda nuestra infraestructura está alojada en AWS, la universalización de los datos ofrecería varias ventajas. Los datos se almacenan en S3, lo que nos permite crear roles y políticas de IAM adecuados para proporcionar acceso granular a ellos. Además, se puede acceder a los datos en S3 a través de diversas instancias de EC2 e infraestructura EKS utilizadas para los cálculos. El Metastore está alojado externamente en RDS y, de nuevo, es de acceso global, mientras que el formato de los datos es Parquet y Delta Lake. Al permitir el acceso externo al Metastore y garantizar que los datos siempre se conserven en formato Parquet, los usuarios pueden utilizar diferentes tipos de marcos de ejecución para lograr sus objetivos. El equipo de aprendizaje automático puede utilizar MLlib y AWS SageMaker para crear y entrenar modelos, y el equipo de análisis puede utilizar Athena para ejecutar consultas interactivas y crear paneles de control sin tener que reingerir los datos en un formato específico de Athena. Las consultas por lotes se ejecutan mediante Spark para crear vistas y proyecciones. Los datos conservados por cualquiera de los marcos de ejecución pueden ser reutilizados por otros.

Un ROI clave es que la frescura de los datos está determinada por la infraestructura de ingesta, ya que todos los consumidores leen los datos directamente del lago de datos. Al basar la infraestructura de ingesta en eventos, los datos casi en tiempo real están disponibles en el lago de datos. Cualquier vista o proyección creada por un marco de ejecución está disponible para su consumo por otro marco. Dado que los gastos operativos son un factor importante en el entorno de la nube, esta arquitectura reduce el gasto al reducir significativamente la cantidad de trabajos por lotes necesarios para copiar los datos en diferentes tipos de entornos de ejecución.
