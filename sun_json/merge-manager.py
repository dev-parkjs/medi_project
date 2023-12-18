###
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

###
spark = SparkSession.builder.appName("CSV Merge").getOrCreate()

###
root_path = "/Users/parkjisook/Desktop/yeardream/medistream/js/sun_json"
csvs_path = f"{root_path}/output"
save_path = f"{root_path}/result"

###
df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(csvs_path + "/*.csv")
df = df.dropDuplicates()
df.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding", "UTF-8").csv(save_path)

###
spark.stop()