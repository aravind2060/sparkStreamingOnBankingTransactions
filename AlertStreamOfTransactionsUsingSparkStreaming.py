import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def initialize_spark_session(app_name="Banking Transactions Alert System"):
    logging.info("Starting to initialize Spark session.")
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    logging.info("Spark session initialized successfully.")
    return spark

def read_streaming_data(spark, folder_path):
    logging.info(f"Preparing to read streaming data from: {folder_path}")
    df = spark \
        .readStream \
        .format("csv") \
        .option("header", "true") \
        .schema("transaction_time TIMESTAMP, account_number STRING, transaction_type STRING, amount DOUBLE, source_file STRING") \
        .load(folder_path)
    logging.info("Streaming DataFrame has been set up.")
    return df

def alert_high_value_transactions(transactions_df):
    logging.info("Setting up alert for high-value transactions (amount > $8,000).")
    return transactions_df.filter("amount > 8000")

def alert_frequent_transactions(transactions_df):
    logging.info("Setting up alert for frequent transactions (more than 3 in the last hour).")
    return transactions_df \
        .groupBy(
            col("account_number"),
            window(col("transaction_time"), "1 hour")
        ) \
        .count() \
        .filter("count > 3")

def alert_suspicious_withdrawals(transactions_df):
    logging.info("Setting up alert for suspicious withdrawals (over $20,000 in last day).")
    return transactions_df \
        .where(col("transaction_type") == "Withdrawal") \
        .withWatermark("transaction_time", "1 day") \
        .groupBy("account_number") \
        .sum("amount") \
        .where(col("sum(amount)") > 20000) \
        .select("account_number", "sum(amount)")

def process_batch(df, epoch_id, alert_type,outputPath):
    logging.info(f"Processing batch for {alert_type} alerts")
    if 'source_file' in df.columns:
        sources = df.select("source_file").distinct().collect()
        logging.info(f"Batch details from {alert_type}: Files - {[src['source_file'] for src in sources]}")
    df.show(truncate=False)  # Show DataFrame for debug purposes
    # df.write.format("csv").mode("append").option("path", outputPath).save()


def start_queries(spark, *queries):
    logging.info("Monitoring started for all configured streaming queries.")
    for query in queries:
        query.awaitTermination()

def main():
    logging.info("Main function execution started.")
    spark = initialize_spark_session()
    transactions_df = read_streaming_data(spark, "/workspaces/sparkStreamingOnBankingTransactions/stream/input/")

    high_value_transactions = alert_high_value_transactions(transactions_df)
    frequent_transactions = alert_frequent_transactions(transactions_df)
    suspicious_withdrawals = alert_suspicious_withdrawals(transactions_df)

    # Setting up streaming queries
    query1 = high_value_transactions.writeStream.outputMode("update").foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "High-value","/workspaces/sparkStreamingOnBankingTransactions/stream/output/")).start()
    query2 = frequent_transactions.writeStream.outputMode("update").foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "Frequent","/workspaces/sparkStreamingOnBankingTransactions/stream/output/")).start()
    query3 = suspicious_withdrawals.writeStream.outputMode("update").foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, "Suspicious withdrawals","/workspaces/sparkStreamingOnBankingTransactions/stream/output/")).start()

    start_queries(spark, query1, query2, query3)

if __name__ == "__main__":
    main()
