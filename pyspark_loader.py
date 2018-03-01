from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()

transactionFile = "/data/fraud/preprocessed/transactions.parquet"
edgeFile = "/data/fraud/preprocessed/edges.parquet"

raw = spark.read.parquet(transactionFile)
raw = raw.filter(raw.nameOrig.isNotNull()) # TODO persisted parquet can contain null (not sure why)
rawEdges = spark.read.parquet(edgeFile)
rawEdges = rawEdges.filter(rawEdges.type.isNotNull()) # TODO persisted parquet can contain null (not sure why)

# Create transaction vertices
transactions = raw.withColumn("~label", lit("transaction")) \
                  .select("~label", "tranId",
                          "step", "type", "amount",
                          col("isFraud").cast("boolean"), col("isFlaggedFraud").cast("boolean"))
transactions.write.format("com.datastax.bdp.graph.spark.sql.vertex") \
                  .option("graph", "fraud") \
                  .mode("append") \
                  .save()

# Create customer vertices
origCustomers = raw.select(col("nameOrig").alias("_id"))
destCustomers = raw.select(col("nameDest").alias("_id"))
customers = origCustomers.unionAll(destCustomers).dropDuplicates() \
                         .withColumn("~label", lit("customer"))
customers.write.format("com.datastax.bdp.graph.spark.sql.vertex") \
               .option("graph", "fraud") \
               .mode("append") \
               .save()

# Create 'balance' vertices
balances = raw.rdd.flatMap(lambda r: [("balance", r["nameOrig"], r["step"], r["newbalanceOrig"]),
                                      ("balance", r["nameDest"], r["step"], r["newbalanceDest"])]) \
                  .toDF(["~label", "_id", "step", "amount"])
balances.write.format("com.datastax.bdp.graph.spark.sql.vertex") \
              .option("graph", "fraud") \
              .mode("append") \
              .save()

### Edges
# Read back all vertices
vertices = spark.read.format("com.datastax.bdp.graph.spark.sql.vertex") \
                     .option("graph", "fraud") \
                     .load()

# from edges
transactionVertices = vertices.filter(col("~label") == "transaction").select("id", "tranId")
customerVertices = vertices.filter(col("~label") == "customer").select("id", "_id")
fromEdges = rawEdges.filter(col("~label") == "from")
fromEdges = fromEdges.join(customerVertices, col("src") == col("_id")) \
                     .select(col("id").alias("src"), "dst", "~label") \
                     .join(transactionVertices, col("dst") == col("tranId")) \
                     .select("src", col("id").alias("dst"), "~label")
fromEdges.write.format("com.datastax.bdp.graph.spark.sql.edge") \
               .option("graph", "fraud") \
               .mode("append") \
               .save()

# to edges
toEdges = rawEdges.filter(col("~label") == "to")
toEdges = toEdges.join(transactionVertices, col("src") == col("tranId")) \
                 .select(col("id").alias("src"), "dst", "~label") \
                 .join(customerVertices, col("dst") == col("_id")) \
                 .select("src", col("id").alias("dst"), "~label")
toEdges.write.format("com.datastax.bdp.graph.spark.sql.edge") \
             .option("graph", "fraud") \
             .mode("append") \
             .save()

# has balance edges
hasEdges = vertices.filter(col("~label") == "balance").select(col("id").alias("balanceId"), col("_id").alias("cusId")) \
                   .join(customerVertices, col("cusId") == col("_id")) \
                   .select(col("id").alias("src"), col("balanceId").alias("dst"), lit("has").alias("~label"))
hasEdges.write.format("com.datastax.bdp.graph.spark.sql.edge") \
              .option("graph", "fraud") \
              .mode("append") \
              .save()