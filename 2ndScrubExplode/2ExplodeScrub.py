
# Scrub
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col,array,when
from pyspark.sql.types import ArrayType, IntegerType, ShortType


spark = SparkSession.builder\
    .appName('ETL')\
        .config("spark.driver.memory", "4g").getOrCreate()
spark



network = spark.read.option("multiline","true").json("IgnoreFolder/df1.json")
provider = spark.read.option("multiline","true").json("IgnoreFolder/df2.json")

type(network)
type(provider)

network.printSchema()
provider.printSchema()

network_exploded = network.withColumn("rates", explode("negotiated_rates"))
id_exploded = network_exploded.withColumn("id", explode("rates.provider_references"))
network_rates = id_exploded.withColumn("prices",explode("rates.negotiated_prices"))

in_network= network_rates.select(
    "billing_code",
    "billing_code_type",
    "negotiation_arrangement",
    col("id").alias("provider_group_id"),
    col("prices.negotiated_type").alias("negotiated_type"),
    col("prices.negotiated_rate").alias("negotiated_rate"),
    col("prices.billing_class").alias("billing_class"),
    col("prices.billing_code_modifier").alias("billing_code_modifier"),
    col("prices.service_code").alias("service_code")
)

in_network.printSchema()


in_network.show()



provider_exploded = provider.withColumn("provider", explode("provider_groups"))
provider_npi = provider_exploded.withColumn("npi", explode("provider.npi"))

in_provider = provider_npi.select(
    col("provider_group_id"),
    col("npi").alias("npi"),
    col("provider.tin.type").alias("tin_type"),
    col("provider.tin.value").alias("tin")
)

in_provider.show(truncate=False)


provider_final=in_provider.withColumn("tin_type",when(col("tin_type")=="ein",1)
                                      .when(col("tin_type")== "npi",2))

provider_final.show()


# Remove null value
from pyspark.sql.functions import expr

new_network=in_network.filter(in_network.billing_code.isNotNull())


# hash_network=remove_network.withColumn('service_code',hash('service_code'))


in_provider_hyphen = provider_final.withColumn("tin",expr("REPLACE(tin,'-','')"))

new_network.filter(in_network.billing_code_modifier.isNotNull()).count()


# in_network_renamed=hash_network.withColumnRenamed('billing_class','bcIs')\
#             .withColumnRenamed('billing_code','bC')\
#             .withColumnRenamed('billing_code_type','bCT')\
#             .withColumnRenamed('negotiated_rate','negR')\
#             .withColumnRenamed('negotiated_type','negT')\
#             .withColumnRenamed('negotiation_arrangement','negA')\
#             .withColumnRenamed('service_code','poSH')
# in_network_renamed.show()


new_network.printSchema()


# Cast to array of integer


provider_cast = in_provider_hyphen.withColumn("tin_type",col("tin_type").cast(ShortType()))

provider_cast.printSchema()


in_network_cast = new_network.withColumn("service_code",col("service_code").cast(ArrayType(IntegerType()))
)

in_network_cast.printSchema()


print(in_network_cast)


type(in_network_cast)
type(provider_cast)

in_network_cast.write.parquet('IgnoreFolder/NetworkScrub.parquet')
provider_cast.write.parquet('IgnoreFolder/ProviderScrub.parquet')





