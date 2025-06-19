from pyspark.sql.functions import col

#Origin of SAP UDL Data, querying data with the SAP structure is very generic, so the names of tables and columns are normally the same in most data architectures,
#unless a change is implemented by direct business rules in the SAP structure.

#Tables----------------------
vbrp = spark.read.format('delta').load('Table/VBRP/Processed_Parquet/')
vbrp.createOrReplaceTempView('vbrp')

vbrk = spark.read.format('delta').load('Table/VBRK/Processed_Parquet/')
vbrk.createOrReplaceTempView('vbrk')

vbak = spark.read.format('delta').load('Table/VBAK/Processed_Parquet/')
vbak.createOrReplaceTempView('vbak')

vbap = spark.read.format('delta').load('Table/VBAP/Processed_Parquet/')
vbap.createOrReplaceTempView('vbap')

#Consult data by Orders and Invoices Numbers--------------------
  #SQL: 
    #Select and Filters:
%sql
Select * from vbak where VKORG like "%UY%" and ERDAT like "202501%" and VBELN in ("N_Order")

    #Small JOIN:
%sql
with tmpview AS ( SELECT vbrp.*, vbak.augru FROM vbrp inner join vbak on vbrp.AUBEL = vbak.vbeln )
select vbeln, augru FROM TMPview
where vbeln in ("345908703"); 

    #Small JOIN and Filters:
%sql
with tmpview AS ( SELECT vbrp.*, vbak.augru FROM vbrp inner join vbak on vbrp.AUBEL = vbak.vbeln )
select    
    aland,
    spart,
    vbeln,
    augru,
    erdat
FROM TMPview
where aland like '%DASH%' and spart in ('00', '11') and erdat >= '20250203' 

    #Join with more tables, and another way to work with TMP Views
%sql
create or replace temp view tmp_invoice_orders as
select
vbrk.vbeln factura,
vbrp.posnr item_factura,
vbak.vbeln order,
vbap.posnr item_pedido
vbak.erdat order_create_date, 
vbak.auart order_type,
vbak.augru reason_code,
vbak.bstnk n_pedido_cliente,
vbak.vkorg sales_org,
vbak.spart division
from
    vbrk
INNER JOIN VBRP ON VBRP.VBELN = VBRK.VBELN
INNER JOIN VBAK ON VBAK.VBELN = VBRP.AUBEL
where vbak.vkorg like 'OO%'

Select  * from tmp_invoice_orders where order_create_date like "202501%" and sales_org in ('QQ','UQ', 'UU') and division in ('12', '13') and order = "5685836734567"



  #PySpark: 
    #Select and Filters:
vbrp\
    .withColumn('ERDAT', col('ERDAT').cast('bigint'))\
    .filter((col('ERDAT')>=20250201) & (col('ERDAT')<=20250231))\
    .filter(col('ALAND').contains('CO'))\
    .filter((col('SPART').contains('5'))|(col('SPART').contains('100'))| (col('SPART').contains('61')))\
    .withColumn('VBELN', col('VBELN').cast('bigint'))\
    .filter(col('VBELN').in("3409845674"))\
    .select('AUBEL').distinct()
display(vbrp)

    #Sum:
from pyspark.sql.functions import sum 
df2 = df1.agg(sum("`sum(COL)`").alias('Total_Value'))
display(df2)









