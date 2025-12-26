from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from resources.dev import config
from scripts.main.utility.encrypt_decrypt import decrypt
from scripts.main.write.database_write import DatabaseWriter

# Calculation for sales team mart
# Find out the total sales done by each sales person every month
# Give the top performer a 1% incentive of total sales of the month
# Write the data into MySQL table

def sales_mart_calculation_table_write(final_sales_data_mart_df):

    window = Window.partitionBy(col("store_id"), col("sales_person_id"), col("sales_date_year"), col("sales_date_month"))

    total_monthly_sales = final_sales_data_mart_df\
        .withColumn("sales_date_year", year(col("sales_date"))) \
        .withColumn("sales_date_month", month(col("sales_date"))) \
        .withColumn("total_sales", sum(col("total_cost")).over(window))\
        .select(col("store_id"),
                col("sales_person_id"),
                concat(col("sales_person_first_name"), lit(" "), col("sales_person_last_name"))
                .alias("full_name"),
                col("sales_date_year"),
                col("sales_date_month"),
                col("total_sales"))\
        .distinct()

    window = Window.partitionBy(col("store_id"), col("sales_date_year"), col("sales_date_month"))\
                   .orderBy(col("total_sales").desc())

    result_df = total_monthly_sales\
        .withColumn("dense_rank", dense_rank().over(window))\
        .withColumn("incentive", when(col("dense_rank") == 1, round(col("total_sales") * 0.01, 2))
                    .otherwise(0))\
        .select(col("store_id"),
                col("sales_person_id"),
                col("full_name"),
                col("sales_date_year"),
                col("sales_date_month"),
                col("total_sales"),
                col("incentive"))

    result_df.persist(StorageLevel.MEMORY_AND_DISK)
    result_df.show()

    # Write the Data into MySQL customers_data_mart table
    db_writer = DatabaseWriter(url = config.url, properties = config.properties)
    db_writer.write_dataframe(result_df, config.sales_team_data_mart_table)
    result_df.unpersist(blocking=True)