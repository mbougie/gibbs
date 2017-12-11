import pyspark as spark


spark.read.csv(
    "D:\\projects\\ksu\\v1\\sample_il_4152_final.csv", header=True, mode="DROPMALFORMED", schema=schema
)