vbrp = spark.read.format('delta').load('Table/VBRP/Processed_Parquet/')
vbrp.createOrReplaceTempView('vbrp')

vbrk = spark.read.format('delta').load('Table/VBRK/Processed_Parquet/')
vbrk.createOrReplaceTempView('vbrk')

vbak = spark.read.format('delta').load('Table/VBAK/Processed_Parquet/')
vbak.createOrReplaceTempView('vbak')

vbap = spark.read.format('delta').load('Table/VBAP/Processed_Parquet/')
vbap.createOrReplaceTempView('vbap')
