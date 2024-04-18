import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, window, col

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def initialize_spark_session(app_name="Banking Transactions Alert System"):
    """
    Initialize and return a Spark session for structured streaming.

    This function sets up the Spark session with the given application name.
    It ensures that the Spark session is available for use in the subsequent steps.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    logging.info(f"Initializing Spark session for '{app_name}'")
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_streaming_data(spark, folder_path):
    """
    Create a streaming DataFrame by reading data from CSV files in a specified folder.

    This function sets up a streaming DataFrame by reading data from CSV files
    located in the specified folder path. It configures the schema for the
    transaction data, including the column names and data types.

    Args:
        spark (SparkSession): Spark session.
        folder_path (str): Path to the directory containing the CSV files.

    Returns:
        DataFrame: Streaming DataFrame configured to read from CSV files.
    """
    logging.info(f"Reading streaming data from '{folder_path}'")
    return spark \
        .readStream \
        .format("csv") \
        .option("header", "true") \
        .schema("transaction_time TIMESTAMP, account_number STRING, transaction_type STRING, amount DOUBLE") \
        .load(folder_path)

def alert_high_value_transactions(transactions_df):
    """
    Filter transactions that have an amount greater than $8,000.

    This function takes the streaming DataFrame of transactions and filters
    out the transactions with an amount exceeding $8,000. The filtered
    DataFrame can then be used to generate alerts for high-value transactions.

    Args:
        transactions_df (DataFrame): DataFrame containing transactions data.

    Returns:
        DataFrame: Filtered DataFrame with high-value transactions.
    """
    logging.info("Generating alerts for high-value transactions")
    return transactions_df.filter("amount > 8000")

def alert_frequent_transactions(transactions_df):
    """
    Identify accounts with more than 3 transactions in the last hour.

    This function takes the streaming DataFrame of transactions and groups
    them by the account number and a 1-hour window based on the transaction
    time. It then filters the DataFrame to only include accounts that have
    more than 3 transactions within the last hour.

    Args:
        transactions_df (DataFrame): DataFrame containing transactions data.

    Returns:
        DataFrame: Filtered DataFrame with accounts having frequent transactions.
    """
    logging.info("Generating alerts for frequent transactions")
    return transactions_df \
        .groupBy(
            col("account_number"),
            window(col("transaction_time"), "1 hour")
        ) \
        .count() \
        .filter("count > 3")

def alert_suspicious_withdrawals(transactions_df):
    """
    Alert for accounts with a total withdrawal amount exceeding $20,000 in the last day.

    This function takes the streaming DataFrame of transactions and filters
    for withdrawal transactions. It then groups the transactions by the
    account number and a 1-day window, calculates the total withdrawal
    amount, and filters the DataFrame to include only accounts with a
    total withdrawal amount exceeding $20,000 in the last day.

    Args:
        transactions_df (DataFrame): DataFrame containing transactions data.

    Returns:
        DataFrame: Filtered DataFrame with accounts having suspicious withdrawals.
    """
    logging.info("Generating alerts for suspicious withdrawals")
    return transactions_df \
        .where(col("transaction_type") == "Withdrawal") \
        .withWatermark("transaction_time", "1 day") \
        .groupBy("account_number") \
        .sum("amount") \
        .where(col("sum(amount)") > 20000) \
        .select("account_number", "sum(amount)")

def start_queries(*queries):
    """
    Start streaming queries and wait for their termination.

    This function takes a variable number of streaming queries and starts them.
    It then waits for the termination of all the running queries.

    Args:
        queries: Variable number of streaming queries to execute.
    """
    logging.info("Starting streaming queries")
    for query in queries:
        query.start().awaitTermination()

def main():
    """
    Main function to set up streaming data processing and start alert queries.

    This function initializes the Spark session, reads the streaming data
    from the CSV files, and sets up the different alert conditions. It then
    starts the streaming queries and waits for their termination.
    """
    spark = initialize_spark_session()
    transactions_df = read_streaming_data(spark, "/workspaces/sparkStreamingOnBankingTransactions/stream/")

    high_value_transactions = alert_high_value_transactions(transactions_df)
    frequent_transactions = alert_frequent_transactions(transactions_df)
    suspicious_withdrawals = alert_suspicious_withdrawals(transactions_df)

    query1 = high_value_transactions.writeStream.outputMode("update").format("console")
    query2 = frequent_transactions.writeStream.outputMode("update").format("console")
    query3 = suspicious_withdrawals.writeStream.outputMode("update").format("console")

    start_queries(query1, query2, query3)

if __name__ == "__main__":
    main()