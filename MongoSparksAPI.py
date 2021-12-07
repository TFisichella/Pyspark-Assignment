from pyspark.sql import SparkSession
import pyspark.sql.functions as F

####Function library

def importCsv(spark, csvPath): #OK
    df = spark.read.option("header", "true").options(delimiter=';').csv(csvPath)
    appendDF(df)

def appendDF(df): #OK
    df.write.format("mongo").mode("append").save()

def getAllTransactionByInvoice(spark, invoice): #OK
    pipeline = "{'$match': {'InvoiceNo': '" + invoice + "'}}"
    df = spark.read.format("mongo").option("pipeline", pipeline).load()
    df.show()
    return df

#Return a tuple containing the code of the most sold product with its corresponding quantity
def getMostSoldProduct(spark): #OK
    df = spark.read.format("mongo").load()
    df2 = df.withColumn("QuantityInt", df['Quantity'].cast("Integer"))
    dfSum = df2.groupBy("StockCode").sum("QuantityInt")
    row = dfSum.orderBy(F.desc("sum(QuantityInt)")).take(1)
    print(row)
    return row[0][0], row[0][1] # return StockCode, MaxQuantity
    
# Return the Id of the customer who spent the most money with the corresponding amount
def getBestCustomer(spark): # OK
    df = spark.read.format("mongo").load()
    df2 = df.withColumn("TotalPrice", (df['Quantity'].cast("Integer") * F.regexp_replace('UnitPrice',',','.').cast("double")))
    dfSum = df2.groupBy("CustomerID").sum("TotalPrice")
    row = dfSum.orderBy(F.desc("sum(TotalPrice)")).where(dfSum["CustomerID"].isNotNull()).take(1)
    print(row)
    return row[0][0], row[0][1] # return CustomerID, TotalPrice spent

def getAverageUnitPrice(spark, stockCode): #OK
    df = spark.read.format("mongo").load()
    df = df.withColumn("UnitPriceFloat", F.regexp_replace('UnitPrice',',','.').cast("double"))
    df = df.select(df["Country"], df["UnitPriceFloat"]).where(df["StockCode"] == stockCode)
    df = df.groupBy("Country").avg("UnitPriceFloat")
    
    df.show()
    return df
    
def getPriceQuantityRatio(spark, invoice): #OK
    df = spark.read.format("mongo").load()
    df = df.withColumn("UnitPriceFloat", F.regexp_replace('UnitPrice',',','.').cast("double"))
    df = df.withColumn("QuantityInt", df['Quantity'].cast("Integer"))
    df = df.select(df["InvoiceNo"], df["StockCode"], df["QuantityInt"], df["UnitPriceFloat"]).where(df["InvoiceNo"] == invoice)
    
    df = df.withColumn("PriceQuantityRatio", df["UnitPriceFloat"] / df['QuantityInt']) #check if QuantityInt != 0

    #df = df.groupBy("InvoiceNo").avg("PriceQuantityRatio")
    
    df.show()
    return df
    
# Return a product distribution in all the countries
def getProductDistributionForAllCountries(spark, stockCode): #OK
    df = spark.read.format("mongo").load()
    df = df.withColumn("QuantityInt", df['Quantity'].cast("Integer"))
    df = df.select(df["Country"], df["QuantityInt"]).where(df["StockCode"] == stockCode)
    df = df.groupBy("Country").sum("QuantityInt")
    
    totalQuantity = df.groupBy().sum().collect()[0][0]
    df = df.withColumn("QuantityPercentage", df['sum(QuantityInt)'] / totalQuantity)

    df.show()
    return df

#Return all the product distribution in a county
def getProductsDistributionPerCountry(spark, country): #OK
    df = spark.read.format("mongo").load()
    df = df.withColumn("QuantityInt", df['Quantity'].cast("Integer"))
    df = df.select(df["StockCode"], df["QuantityInt"]).where(df["Country"] == country)
    df = df.groupBy("StockCode").sum("QuantityInt")
    
    totalQuantity = df.groupBy().sum().collect()[0][0]
    df = df.withColumn("QuantityPercentage", df['sum(QuantityInt)'] / totalQuantity)

    df.show()
    return df

# Return a dictionary of all the products distribution for every countries    
def getProductsDistribution(spark): #OK
    df = spark.read.format("mongo").load()
    countryRows = df.select("Country").distinct().collect()
    
    result = {}
    for row in countryRows:
        dfCountry = getProductsDistributionPerCountry(spark, row["Country"])
        result[row["Country"]] = dfCountry
        print(row["Country"])
    
    return result

#Return all the unit prices distribution in a county
def getPricesDistribution(spark, country): #OK
    df = spark.read.format("mongo").load()
    df = df.withColumn("QuantityInt", df['Quantity'].cast("Integer"))
    df = df.select(df["UnitPrice"], df["QuantityInt"]).where(df["Country"] == country)
    df = df.groupBy("UnitPrice").sum("QuantityInt")
    
    totalQuantity = df.groupBy().sum().collect()[0][0]
    df = df.withColumn("QuantityPercentage", df['sum(QuantityInt)'] / totalQuantity)

    df.show()
    return df

##### Unit tests

# This variable needs to be set accordingly depending on the user's Db configuration
my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.executor.memory", "1g") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/DBName.CollectionNameGT") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/DBName.CollectionNameGT") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()
    
# Import data into Db   
importCsv(my_spark, "C:\Path\To\Online Retail.csv")

# Group all transactions by invoice
getAllTransactionByInvoice(my_spark, "536365")

# Which product was sold the most
getMostSoldProduct(my_spark)

# Specify which customer spend the most money
getBestCustomer(my_spark)

# Get the average unit price of the specified product
getAverageUnitPrice(my_spark, "85123A")

# Give the price quantity ratio of an invoice
getPriceQuantityRatio(my_spark, "536365")

# Give the distribution of products in different countries
getProductDistributionForAllCountries(my_spark, "85123A")
getProductsDistributionPerCountry(my_spark, "France")
getProductsDistribution(my_spark)

# Give a distribution of prices in a country but the goal of this exercise wasn't clear
getPricesDistribution(my_spark, "France")