
spark = SparkSession.builder.appName("CustomerTransformations").getOrCreate()
file_path = 'customers-1000.csv'
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(5)


df = df.withColumn("Full Name", concat_ws(" ", col("First Name"), col("Last Name")))


def extract_domain(email):
    return email.split('@')[-1]

extract_domain_udf = udf(extract_domain, StringType())
df = df.withColumn("Email Domain", extract_domain_udf(col("Email")))
 
df = df.filter(year(col("Subscription Date")) == 2021)

country_counts = df.groupBy("Country").count()

df = df.withColumn("Phone 1 Normalized", regexp_replace(col("Phone 1"), "[^0-9]", ""))
df = df.withColumn("Phone 2 Normalized", regexp_replace(col("Phone 2"), "[^0-9]", ""))


df.show(5)

country_counts.show()
