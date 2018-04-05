# DSE Graph example

DSE Graph eamples of loading data and analytics.

## Data set

[Synthetic Financial Datasets For Fraud Detection](https://www.kaggle.com/ntnu-testimon/paysim1)

Simulated mobile money transactions based on a sample of real transactions extracted from one month of financial logs from a mobile money service.

The fraudulent agents inside the simulation inject fraud transaction occasionally (marked in `isFraud` column).

## Graph schema

For complete schema, see schema.groovy.

Use gremlin console to create graph called `fraud`.

```
$ dse gremlin-console -e schema.groovy
```

## Data loading

Data loading for this project is done in two steps.

- Preprocess data
- Load to DSE Graph

### Prerequisites

Download data from Kaggle page linked above and unzip it.
```
$ unzip paysim1.zip
```
In DSE FS, create a directory called `/data/` that contains a directory called `fraud/`. Put `PS_20174392719_1491204439457_log.csv` into DSE FS at `/data/fraud/`; name the file `transactions.csv`: 

```
$ dse fs
dsefs dsefs://127.0.0.1:5598/ > mkdir /data/
dsefs dsefs://127.0.0.1:5598/ > mkdir /data/fraud/
dsefs dsefs://127.0.0.1:5598/ > exit
$ dse fs "put PS_20174392719_1491204439457_log.csv /data/fraud/transactions.csv"
```

### Preprocess data

Preprocessing loads data you put is DSEFS and produces two parquet file.

- `/data/fraud/preprocessed/transactions.parquet`
- `/data/fraud/preprocessed/edges.parquet`

The former file contains transactions with generated transaction ID (`tranId` UUID). The latter contains transaction to customer relationship (from, to) so we can easily add edges later.

To preprocess, submit spark job as follows:

```
$ dse spark-submit preprocess_data.py
```

### Loading to DSE Graph

Once you preprocessed, run the second spark job to load data to DSE Graph.

```
$ dse spark-submit pyspark_loader.py
```

## Start exploring!

Using DataStax Studio 6.0, you can import studio notebook in this repository and see some example analytics using gremlin query.
