import uuid

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit, instr, col

### Preprocess transaction data ###
# This preprocess job gives each transaction an ID,
# and creates two files for later graph construction.
#
# - transactions.parquet
#   transaction data with UUID trancastion ID
# - edges.parquet
#   relationship between transactions and customers
###

spark = SparkSession.builder.getOrCreate()

# input data file
dataFile = "/data/fraud/transactions.csv"

# preprocessed data file
transactionFile = "/data/fraud/preprocessed/transactions.parquet"
edgeFile = "/data/fraud/preprocessed/edges.parquet"

### UDFs ###
# UUID generator for each transaction
gen_uuid = udf(lambda: str(uuid.uuid1()), StringType())

# CSV schema
schema = StructType().add("step", IntegerType()) \
                     .add("type", StringType()) \
                     .add("amount", DecimalType(10,2)) \
                     .add("nameOrig", StringType()) \
                     .add("oldbalanceOrg", DecimalType(10,2)) \
                     .add("newbalanceOrig", DecimalType(10,2)) \
                     .add("nameDest", StringType()) \
                     .add("oldbalanceDest", DecimalType(10,2)) \
                     .add("newbalanceDest", DecimalType(10,2)) \
                     .add("isFraud", IntegerType()) \
                     .add("isFlaggedFraud", IntegerType())

rawTransactions = spark.read.csv(dataFile, header=True, schema=schema) \
                            .withColumn("tranId", gen_uuid())

rawTransactions.write.format("parquet").save(transactionFile)

# reload saved transactions to get the same transaction id
rawTransactions = spark.read.parquet(transactionFile)
rawEdges = rawTransactions.rdd.flatMap(lambda r: [(r["nameOrig"], r["tranId"], "from", r["type"]),
                                                  (r["tranId"], r["nameDest"], "to", r["type"])]) \
                              .toDF(["src", "dst", "~label", "type"])
rawEdges.write.format("parquet").save(edgeFile)