# I decided to solve this problem using PySpark

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import time

start = time.time()

# master("local[*]") will use multiple Machines/Cores
spark = SparkSession.builder.master("local[*]").appName("Deduplication").getOrCreate()

# define names and types of the columns
sch = StructType([StructField('ID', IntegerType(), True),\
                    StructField('Game', StringType(), True),\
                    StructField('Type', StringType(), True),\
                    StructField('Time/Purchase', FloatType(), True)])
# I didn't add 5th column in schema, because as we can check, the 5th column in CSV file contains only 0s
# I considered that working on this (5th) column would extend the time and consume excessive resources especially when dealing with large datasets

# Read file
df = spark.read.schema(sch).csv("steam-200k.csv")

# Group table by first 3 columns, if 'Type' is 'play', in 4th column 'Time/Purchase' we summarize data in this column (play time)
# else if 'Type' is 'purchase' in 4th column 'Time/Purchase' we write just 1 (I considered that for each ID there could be only one purchase)
grouped_df = df.groupBy("ID", "Game", 'Type').agg(
    func.sum(func.when(df["Type"] == "play", df["Time/Purchase"]).otherwise(1)).alias("Time/Purchase")
)

# We command that the output was in single csv file (Result), without header
grouped_df.coalesce(1).write.csv('Result', header=False)

spark.stop()

end = time.time()

# calculates and prints the time needed for the process
print(f'Code takes {end-start} seconds to do a job')
