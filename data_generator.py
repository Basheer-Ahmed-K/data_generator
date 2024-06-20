# Databricks notebook source
# MAGIC %pip install dbldatagen
# MAGIC

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

row_count = 1000 * 100
column_count = 10
testDataSpec = (
    dg.DataGenerator(spark, name="test_data_set1", rows=row_count, partitions=4)
    .withIdOutput()
    .withColumn(
        "r",
        FloatType(),
        expr="floor(rand() * 350) * (86400 + 3600)",
        numColumns=column_count,
    )
    .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
    .withColumn("code2", "integer", minValue=0, maxValue=10, random=True)
    .withColumn("code3", StringType(), values=["online", "offline", "unknown"])
    .withColumn(
        "code4", StringType(), values=["a", "b", "c"], random=True, percentNulls=0.05
    )
    .withColumn(
        "code5", "string", values=["a", "b", "c"], random=True, weights=[9, 1, 1]
    )
)

dfTestData = testDataSpec.build()

display(dfTestData)

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

# Define the schema and generate data for the customers table
customer_row_count = 1000  # Number of rows for customers
customer_spec = (
    dg.DataGenerator(spark, name="customers", rows=customer_row_count, partitions=4)
    .withIdOutput()
    .withColumn("customer_id", IntegerType(), expr="id")  # Use the generated unique ID as customer_id
    .withColumn("customer_name", StringType(), expr="concat('customer_', id)")
    .withColumn("customer_age", IntegerType(), minValue=18, maxValue=80)
)

customers_df = customer_spec.build()

# Define the schema and generate data for the products table
product_row_count = 500  # Number of rows for products
product_spec = (
    dg.DataGenerator(spark, name="products", rows=product_row_count, partitions=4)
    .withIdOutput()
    .withColumn("product_id", IntegerType(),omit=True)
    .withColumn("product_name", StringType(), expr="concat('product_', id)")
    .withColumn("product_price", FloatType(), minValue=10.0, maxValue=1000.0)
)

products_df = product_spec.build()

# Show generated data
print("Customers DataFrame:")
customers_df.show(5)

print("Products DataFrame:")
products_df.show(5)




# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, StringType, FloatType, DateType
from faker import Faker

fake = Faker()

def generate_fake_names(no_of_rows):
    first_names = [fake.first_name() for _ in range(no_of_rows)]
    last_names = [fake.last_name() for _ in range(no_of_rows)]
    return first_names, last_names

def generate_customers(no_of_rows):
    first_names, last_names = generate_fake_names(no_of_rows)
    
    customer_spec = (
        dg.DataGenerator(spark, name="customers", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("CustomerID", IntegerType(), expr="id")  # Generate column data from one or more seed columns
        .withColumn("FirstName", StringType(), values=first_names, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("LastName", StringType(), values=last_names, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("Email", StringType(), expr="concat('customer_', id, '@example.com')")  # Use SQL based expressions to control or augment column generation
    )
    customers_df = customer_spec.build()
    return customers_df

def generate_products(no_of_rows):
    product_spec = (
        dg.DataGenerator(spark, name="products", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("ProductID", IntegerType(), expr="id")  # Generate column data from one or more seed columns
        .withColumn("ProductName", StringType(), values=["ProductA", "ProductB", "ProductC", "ProductD", "ProductE"], random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("Price", FloatType(), minValue=5.0, maxValue=500.0, random=True)  # Specify numeric, time, and date ranges for columns
        .withColumn("Category", StringType(), values=["Category1", "Category2", "Category3"], random=True)  # Generate column data at random or from repeatable seed values
    )
    products_df = product_spec.build()
    return products_df

def generate_purchase_history(no_of_rows, customers_df, products_df):
    customer_ids = customers_df.select("CustomerID").rdd.flatMap(lambda x: x).collect()
    product_ids = products_df.select("ProductID").rdd.flatMap(lambda x: x).collect()
    
    purchase_history_spec = (
        dg.DataGenerator(spark, name="purchase_history", rows=no_of_rows, partitions=4)  # Specify number of rows to generate, Specify number of Spark partitions to distribute data generation across
        .withIdOutput()
        .withColumn("CustomerID", IntegerType(), values=customer_ids, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("ProductID", IntegerType(), values=product_ids, random=True)  # Generate column data at random or from repeatable seed values
        .withColumn("PurchaseDate", DateType(), expr="current_date()")  # Use SQL based expressions to control or augment column generation
        .withColumn("Quantity", IntegerType(), minValue=1, maxValue=10, random=True)  # Specify numeric, time, and date ranges for columns
    )
    purchase_history_df = purchase_history_spec.build()
    return purchase_history_df

no_of_rows = 2000  # Specify number of rows to generate

# Generate data
customers_df = generate_customers(no_of_rows)
products_df = generate_products(no_of_rows)
purchase_history_df = generate_purchase_history(no_of_rows, customers_df, products_df)

# Display the dataframes
print("Customers DataFrame:")
display(customers_df)

print("Products DataFrame:")
display(products_df)

print("PurchaseHistory DataFrame:")
display(purchase_history_df)

