from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
.appName("EmployeePipeline") \
.config("spark.jars", "/opt/jdbc/postgresql-42.7.3.jar") \
.getOrCreate()


# Load CSV
df = spark.read.option("header", True).csv("/opt/data/employees_raw.csv")


# Remove duplicates
df = df.dropDuplicates(["employee_id"])


# Email validation
email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
df = df.filter(col("email").rlike(email_regex))


# Clean salary
df = df.withColumn(
"salary",
regexp_replace(col("salary"), "[$,]", "").cast("double")
)


# Date fixes
df = df.withColumn("hire_date", to_date("hire_date")) \
.filter(col("hire_date") <= current_date())


# Name standardization
df = df.withColumn("first_name", initcap(col("first_name"))) \
.withColumn("last_name", initcap(col("last_name")))


# Email cleanup
df = df.withColumn("email", lower(col("email")))


# Enrichment
df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
.withColumn("email_domain", split(col("email"), "@").getItem(1))


# Age & tenure
df = df.withColumn("birth_date", to_date("birth_date")) \
.withColumn("age", floor(datediff(current_date(), col("birth_date")) / 365.25)) \
.withColumn("tenure_years", round(datediff(current_date(), col("hire_date")) / 365.25, 1))


# Salary bands
df = df.withColumn(
"salary_band",
when(col("salary") < 50000, "Junior")
.when(col("salary").between(50000,80000), "Mid")
.otherwise("Senior")
)