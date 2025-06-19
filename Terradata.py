from pyspark.sql.functions import col
from pyspark.sql.types import *
from delta import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import when
from pyspark.sql.functions import max

SV = (
    spark.read.format("delta")
    .load("/mnt/adls/gen2Prod/")
    .withColumn("Doc_Num", col("Doc_Num").cast("bigint"))
    .filter((col("Sales_Org_Code").contains("456")))
    .filter(
        (col("division_code").contains("12"))
        | (col("division_code").contains("456"))
        | (col("division_code").contains("879"))
    )
    .fillna(
        {
            "Col1": 0,
            "Col2": 0,
            "Col3": 0
        }
    )
    .withColumn(
        "Col1",
        (col("Col2").cast(DecimalType(18, 2)))
        + (col("Col3").cast(DecimalType(18, 2))),
    )
    .withColumn(
        "Col1",
        (col("Col2").cast(DecimalType(18, 2)))
        + (col("Col3").cast(DecimalType(18, 2))),
    )
)



lista_ids = [
5674567456,
6765786578,
9380590396,
7897897689,
6784579436,
06789456734
]

df_filtrado = SV_MX.select("Doc_Num","Net_Invoiced_Value_NIV", "billing_date").filter(col("Doc_Num").isin(lista_ids))
df_filtrado.display()
