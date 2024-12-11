# Databricks notebook source
# MAGIC %pip install dbldatagen --q
# MAGIC %pip install faker --q

# COMMAND ----------

import dbldatagen as dg
import re
from faker import Faker
from faker.providers import address, company, lorem, phone_number

# COMMAND ----------

username = re.sub('[^0-9a-zA-Z]', '_', dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply("user"))

# COMMAND ----------

spark.sql(f'''
  CREATE CATALOG IF NOT EXISTS dlt_workshop_{username};''')
spark.sql(f'''
  CREATE SCHEMA IF NOT EXISTS dlt_workshop_{username}.finance;''')
spark.sql(f'''
  CREATE VOLUME IF NOT EXISTS dlt_workshop_{username}.finance._files;''')

# COMMAND ----------

def initFaker(context):
  context.faker = Faker(locale='en_US')
  context.faker.add_provider(address)
  context.faker.add_provider(company)
  context.faker.add_provider(lorem)
  context.faker.add_provider(phone_number)

name_generator = (lambda context, v: context.faker.company())
address_generator = (lambda context, v: context.faker.address())
phone_generator = (lambda context, v: context.faker.phone_number())
desc_generator = (lambda context, v: context.faker.sentence())

customers_generator = (
  dg.DataGenerator(rows=100)
    .withIdOutput()
    .withColumn('customer_name', 'string', percentNulls=0.01, text=dg.PyfuncText(name_generator, init=initFaker))
    .withColumn('billing_address', 'string', percentNulls=0.01, text=dg.PyfuncText(address_generator, init=initFaker))
    .withColumn('mailing_address', 'string', percentNulls=0.2, text=dg.PyfuncText(address_generator, init=initFaker))
    .withColumn('phone_number', 'string', percentNulls=0.2, text=dg.PyfuncText(phone_generator, init=initFaker))
    .withColumn('email', 'string', percentNulls=0.1, expr='concat(replace(initcap(regexp_replace(customer_name, "[^0-9A-Za-z]", " ")), " ", ""), "@example.com")')
    .withColumn('payment_terms', 'string', values=['DUE_ON_RECEIPT', 'NET_30', 'NET_60', 'NET_90', 'NET_120'], random=True)
    .withColumn('balance_limit', 'decimal(10,2)', percentNulls=0.01, min=1000, max=100000, random=True)
)

suppliers_cdc_generator = (
  dg.DataGenerator(rows=20)
    .withColumn('update_type', 'string', values=['INSERT', 'UPDATE', 'DELETE'], random=True)
    .withColumn('update_date', 'timestamp', minValue='2024-01-01 00:00:00', maxValue='2024-12-31 11:59:59', random=True)
    .withColumn('id', 'integer', minValue=1, maxValue=100, random=True)
    .withColumn('supplier_name', 'string', percentNulls=0.01, text=dg.PyfuncText(name_generator, init=initFaker))
    .withColumn('billing_address', 'string', percentNulls=0.01, text=dg.PyfuncText(address_generator, init=initFaker))
    .withColumn('mailing_address', 'string', percentNulls=0.2, text=dg.PyfuncText(address_generator, init=initFaker))
    .withColumn('phone_number', 'string', percentNulls=0.2, text=dg.PyfuncText(phone_generator, init=initFaker))
    .withColumn('email', 'string', percentNulls=0.1, expr='concat(replace(initcap(regexp_replace(supplier_name, "[^0-9A-Za-z]", " ")), " ", ""), "@example.com")')
)

items_generator = (
  dg.DataGenerator(rows=1000)
    .withIdOutput()
    .withColumn('description', 'string', percentNulls=0.1, text=dg.PyfuncText(desc_generator, init=initFaker))
    .withColumn('unit_price', 'decimal(10,2)', percentNulls=0.01, min=1, max=500, step=0.01, random=True)
)

orders_generator = (
  dg.DataGenerator(rows=10000)
    .withIdOutput()
    .withColumn('order_date', 'timestamp', minValue='2020-01-01 00:00:00', maxValue='2024-01-01 00:00:00', random=True)
    .withColumn('description', 'string', percentNulls=0.1, text=dg.PyfuncText(desc_generator, init=initFaker))
    .withColumn('customer_id', 'integer', minValue=1, maxValue=100, random=True)
    .withColumn('item_id', 'integer', minValue=1, maxValue=1000, random=True)
    .withColumn('qty_ordered', 'integer', minValue=1, maxValue=100, random=True)
    .withColumn('business_unit', 'string', values=['retail', 'wholesale'], random=True)
)

orders_new_generator = (
  dg.DataGenerator(rows=1000)
    .withIdOutput()
    .withColumn('order_date', 'timestamp', minValue='2020-01-01 00:00:00', maxValue='2024-01-01 00:00:00', random=True)
    .withColumn('description', 'string', percentNulls=0.1, text=dg.PyfuncText(desc_generator, init=initFaker))
    .withColumn('customer_id', 'integer', minValue=1, maxValue=100, random=True)
    .withColumn('item_id', 'integer', minValue=1, maxValue=1000, random=True)
    .withColumn('qty_ordered', 'integer', minValue=1, maxValue=100, random=True)
    .withColumn('business_unit', 'string', values=['direct_to_consumer'])
)

orders_backlog_generator = orders_generator.withRowCount(100000) \
  .withColumn('order_date', 'timestamp', minValue='2000-01-01 00:00:00', maxValue='2020-01-01 00:00:00', random=True)

# COMMAND ----------

customers_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', '|') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/customers')

# COMMAND ----------

suppliers_cdc_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', ',') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/suppliers_cdc')

# COMMAND ----------

items_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', ',') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/items')

# COMMAND ----------

orders_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', ',') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/orders')

# COMMAND ----------

orders_new_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', ',') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/orders_new')

# COMMAND ----------

orders_backlog_generator.build() \
  .write \
  .format('csv') \
  .option('header', 'true') \
  .option('sep', ',') \
  .mode('overwrite') \
  .save(f'/Volumes/dlt_workshop_{username}/finance/_files/orders_backlog')
