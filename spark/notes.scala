import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType};

val LineOrderSchema = StructType(Seq(
    StructField("LO_ORDERKEY", LongType, true),
    StructField("LO_LINENUMBER", LongType, true),
    StructField("LO_CUSTKEY", IntegerType, true),
    StructField("LO_PARTKEY", IntegerType, true),
    StructField("LO_SUPPKEY", IntegerType, true),
    StructField("LO_ORDERDATE", IntegerType, true),
    StructField("LO_ORDERPRIOTITY", StringType, true),
    StructField("LO_SHIPPRIOTITY", IntegerType, true),
    StructField("LO_QUANTITY", LongType, true),
    StructField("LO_EXTENDEDPRICE", LongType, true),
    StructField("LO_ORDTOTALPRICE", LongType, true),
    StructField("LO_DISCOUNT", LongType, true),
    StructField("LO_REVENUE", LongType, true),
    StructField("LO_SUPPLYCOST", LongType, true),
    StructField("LO_TAX", LongType, true),
    StructField("LO_COMMITDATE", IntegerType, true),
    StructField("LO_SHIPMODE", StringType, true),
    StructField("TMP", StringType, true)
    ))

val d1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option("inferSchema","true").option("quote",null).schema(customSchema).load("/mnt/md0/ssb/lineorder.tbl")


val DateSchema = StructType(Seq(
    StructField("D_DATEKEY", IntegerType, true),
    StructField("D_DATE", StringType, true),
    StructField("D_DAYOFWEEK", StringType, true),
    StructField("D_MONTH", StringType, true),
    StructField("D_YEAR", IntegerType, true),
    StructField("D_YEARMONTHNUM", IntegerType, true),
    StructField("D_YEARMONTH", StringType, true),
    StructField("D_DAYNUMINWEEK", IntegerType, true),
    StructField("D_DAYNUMINMONTH", IntegerType, true),
    StructField("D_DAYNUMINYEAR", IntegerType, true),
    StructField("D_MONTHNUMINYEAR", IntegerType, true),
    StructField("D_WEEKNUMINYEAR", IntegerType, true),
    StructField("D_SELLINGSEASON", StringType, true),
    StructField("D_LASTDAYINWEEKFL", IntegerType, true),
    StructField("D_LASTDAYINMONTHFL", IntegerType, true),
    StructField("D_HOLIDAYFL", IntegerType, true),
    StructField("D_WEEKDAYFL", IntegerType, true),
    StructField("TMP", StringType, true)
    ))

val d2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option("inferSchema","true").option("quote",null).schema(DateSchema).load("/mnt/md0/ssb/date.tbl")

val PartSchema = StructType(Seq(
    StructField("P_PARTKEY", IntegerType, true),
    StructField("P_NAME", StringType, true),
    StructField("P_MFGR", StringType, true),
    StructField("P_CATEGORY", StringType, true),
    StructField("P_BRAND1", StringType, true),
    StructField("P_COLOR", StringType, true),
    StructField("P_TYPE", StringType, true),
    StructField("P_SIZE", IntegerType, true),
    StructField("P_CONTAINER", StringType, true),
    StructField("TMP", StringType, true)
    ))

val d2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option("inferSchema","true").option("quote",null).schema(PartSchema).load("/mnt/md0/ssb/part.tbl")

val SUPPLIERSchema = StructType(Seq(
    StructField("S_SUPPKEY", IntegerType, true),
    StructField("S_NAME", StringType, true),
    StructField("S_ADDRESS", StringType, true),
    StructField("S_NATION_PREFIX", StringType, true),
    StructField("S_NATION", StringType, true),
    StructField("S_REGION", StringType, true),
    StructField("S_PHONE", StringType, true),
    StructField("TMP", StringType, true)
    ))

val dSUPPLIER = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option("inferSchema","true").option("quote",null).schema(SUPPLIERSchema).load("/mnt/md0/ssb/supplier.tbl")

val CustomerSchema = StructType(Seq(
    StructField("C_CUSTKEY", IntegerType, true),
    StructField("C_NAME", StringType, true),
    StructField("C_ADDRESS", StringType, true),
    StructField("C_NATION_PREFIX", StringType, true),
    StructField("C_NATION", StringType, true),
    StructField("C_REGION", StringType, true),
    StructField("C_PHONE", StringType, true),
    StructField("C_MKTSEGMENT", StringType, true),
    StructField("TMP", StringType, true)
    ))

val dCustomer = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option("inferSchema","true").option("quote",null).schema(CustomerSchema).load("/mnt/md0/ssb/customer.tbl")


d1.write.parquet("/mnt/i3600/spark/ssb")
d2.write.parquet("/mnt/i3600/spark/ssb-date")
d2.write.parquet("/mnt/i3600/spark/ssb-part")
dSUPPLIER.write.parquet("/mnt/i3600/spark/ssb-supplier")
dCustomer.write.parquet("/mnt/i3600/spark/ssb-customer")

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

val pLineOrder = sqlContext.read.parquet("/mnt/i3600/spark/ssb")
val pDate = sqlContext.read.parquet("/mnt/i3600/spark/ssb-date")
val pPart = sqlContext.read.parquet("/mnt/i3600/spark/ssb-part")
val pSupplier = sqlContext.read.parquet("/mnt/i3600/spark/ssb-supplier")
val pCustomer = sqlContext.read.parquet("/mnt/i3600/spark/ssb-customer")


pLineOrder.registerTempTable("lineorder")
pDate.registerTempTable("date")
pPart.registerTempTable("part")
pSupplier.registerTempTable("supplier")
pCustomer.registerTempTable("customer")

val sql=sqlContext.sql("select sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue from lineorder,date where LO_ORDERDATE = D_DATEKEY and D_YEAR = 1993 and LO_DISCOUNT between 1 and 3 and LO_QUANTITY < 25")

val sql=sqlContext.sql("select sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue from lineorder,date where LO_ORDERDATE = D_DATEKEY and D_YEARMONTHNUM = 199401  and LO_DISCOUNT between 4 and 6 and LO_QUANTITY between 26 and 35")

val sql13=sqlContext.sql("select sum(LO_EXTENDEDPRICE*LO_DISCOUNT) as revenue from lineorder,date where LO_ORDERDATE = D_DATEKEY and D_WEEKNUMINYEAR = 6 and D_YEAR = 1994 AND LO_QUANTITY between 36 and 40 AND LO_DISCOUNT between 5 and 7")


val sql21=sqlContext.sql("select sum(LO_REVENUE), D_YEAR, P_BRAND1 from lineorder, date, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_CATEGORY = 'MFGR#12' and S_REGION = 'AMERICA' group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1")

val sql22=sqlContext.sql("select sum(LO_REVENUE), D_YEAR, P_BRAND1 from lineorder, date, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 between 'MFGR#2221' and 'MFGR#2228' and S_REGION = 'ASIA'  group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1")

val sql23=sqlContext.sql("select sum(LO_REVENUE), D_YEAR, P_BRAND1 from lineorder, date, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 = 'MFGR#2339' and S_REGION = 'EUROPE' group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1")


val sql31=sqlContext.sql("select C_NATION, S_NATION, D_YEAR, sum(LO_REVENUE) as revenue from customer, lineorder, supplier, date where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY and LO_ORDERDATE = D_DATEKEY and C_REGION = 'ASIA' and S_REGION = 'ASIA' and D_YEAR >= 1992 and D_YEAR <= 1997 group by C_NATION, S_NATION, D_YEAR order by D_YEAR asc, revenue desc")



