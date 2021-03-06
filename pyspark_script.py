import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType

# Создаем Spark сессию
spark = SparkSession.builder.appName('Test_Be').getOrCreate()

# Создаем структуру данных для входных csv. Затем передаем структуру в параметр schema
customer_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("joinDate", DateType(), True),
    StructField("status", StringType(), True),
])

product_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("numberOfProducts", IntegerType(), True)
])

order_schema = StructType([
    StructField("customerID", IntegerType(), True),
    StructField("orderID", IntegerType(), True),
    StructField("productID", IntegerType(), True),
    StructField("numberOfProduct", IntegerType(), True),
    StructField("orderDate", DateType(), True),
    StructField("status", StringType(), True)
])

# Чтение входных данных
df_customers = spark.read.option("sep", "\t").schema(customer_schema).csv('customer.csv')
df_products = spark.read.option("sep", "\t").schema(product_schema).csv('product.csv')
df_orders = spark.read.option("sep", "\t").schema(order_schema).csv('order.csv')

df_customers.show()
df_products.show()
df_orders.show()

#Создадим временное представление наших фреймов данных
df_customers.createOrReplaceTempView('df_customers')
df_orders.createOrReplaceTempView('df_orders')
df_products.createOrReplaceTempView('df_products')

# Пишем запрос для нахождения самого популярного продукта для каждого клиента
query = """
SELECT customer_name, product_name
FROM ( SELECT c.name as customer_name, p.name as product_name, SUM(o.numberOfProduct), 
       RANK() OVER (PARTITION BY c.name ORDER BY SUM(o.numberOfProduct) DESC) as r_n
       FROM df_orders o
       JOIN df_products p ON o.productID = p.id 
       JOIN df_customers c ON c.id = o.customerID
       GROUP BY c.name, p.name 
     ) as V
WHERE V.r_n = 1
ORDER BY customer_name 
"""
# Применим наш sql запрос и выведем результат
result_df = spark.sql(query)

result_df.show()

# Запись результата в csv файл
result_df.write.csv('result.csv') # или result_df.toPandas().to_csv('result.csv')
