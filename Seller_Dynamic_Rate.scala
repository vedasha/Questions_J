
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._


object Seller_Dynamic_Rate {
  
  
  
  
  
  def main(args: Array[String]) {
    
   Logger.getLogger("org").setLevel(Level.ERROR)
    
   val spark =  SparkSession.builder.appName("Seller_Dynamic_Rate").master("local[*]")
    .config("spark.sql.warehouse.dir","/Users/Hive/spark-warehouse").
    enableHiveSupport().
    getOrCreate()
    //val sc = spark.sparkContext
    //val competitor_data = sc.textFile("Users/vm047138/Spark/Data/SparkScala/get_competitor_data.csv")
    
 import spark.implicits._
    import org.apache.spark.sql.types._
    val schemaRdd = StructType(Array(StructField("product_id", IntegerType, true),
        StructField("price", IntegerType, true),
        StructField("rivalName", StringType, true),
        StructField("sale_category", StringType, true),
        StructField("classification", StringType, true)))
        
        
        
        val schemaRdd1 = StructType(Array(StructField("product_id", IntegerType, true),
        StructField("pc", IntegerType, true),
        StructField("min_margin", IntegerType, true),
        StructField("max_margin", IntegerType, true)))
        
    val competitor_data = spark.read.format("csv").option("header", "true").
    schema(schemaRdd)
    .load("/Users/vm047138/Spark/Data/SparkScala/get_competitor_data.csv")
    
    
    val my_data = spark.read.format("csv").option("header", "true").schema(schemaRdd1)
    .load("/Users/vm047138/Spark/Data/SparkScala/product_cost_data.csv")
    
    competitor_data.createOrReplaceTempView("competitor")
    
    my_data.createOrReplaceTempView("my_data")
    
    
        


 val min_price = spark.
 sql("select new.product_id,new.price,rivalName,classification,sale_category from (Select c.PRODUCT_ID,min(c.price) as price from my_data m left join competitor c on m.product_id=c.product_id group by c.product_id) new join competitor where new.product_id = competitor.product_id  and new.price=competitor.price").toDF()

   min_price.createOrReplaceTempView("with_min_table")
 //min_price.printSchema()
 
 val all_details = spark.sql("select my_data.product_id,pc,max_margin,min_margin,classification,sale_category,price as q,rivalName  from with_min_table join my_data on my_data.product_id = with_min_table.product_id").toDF()

 all_details.createOrReplaceTempView("all_details")
//all_details.printSchema()
 
  spark.sql("SELECT  product_id,CASE WHEN (pc + max_margin) < q THEN (pc + max_margin) WHEN (pc + min_margin) < q  then q when ((pc < q) and upper(sale_category) == \"SPECIAL\" ) then q WHEN ((q < pc) and upper(sale_category) == \"SPECIAL\" and upper(classification) == \"VERYHIGH\") THEN 0.9*pc ELSE pc END as FinalPrice,now() as TimeStamp,q as Cheapest_Price_Among_Rivals,rivalName as rival_name FROM all_details").show()
 
 //competitor_data.printSchema()
 
 
 //competitor_data.write.saveAsTable("competitor_data")
 

spark.sql("create database IF NOT EXISTS seller_Dynamic_rating")
 spark.sql("use  seller_Dynamic_rating")
 //spark.sql("drop table competitor_partition ")
 spark.sql("create table IF NOT EXISTS competitor_partition (productid INT,price DOUBLE,saleevent STRING,fetchts STRING)  PARTITIONED BY (rivalname STRING)")

//spark.sql("show databases").show()
spark.sql("show tables").show()

 
 spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
  spark.sql("SET hive.exec.dynamic.partition=true")
 
  //spark.sql("describe table extended competitor_partition").show()
spark.sql("INSERT OVERWRITE TABLE competitor_partition partition(rivalname) select product_id ,price,sale_category ,now() ,regexp_replace(rivalName, \".com\", \"_com\") as rivalname  from competitor")
 
 spark.sql("SHOW partitions competitor_partition").show()
  spark.sql("SELECT * FROM   competitor_partition").show()
 
spark.sql("select product_id AS productId,price,sale_category as saleEvent,now() as fetchTS,regexp_replace(rivalName, \".com\", \"_com\") as rivalName from competitor").show()
 
 
  val competitor_data2 = spark.read.format("csv").option("inferSchema","true").
   option("header", "true").
    load("/Users/vm047138/Spark/Data/SparkScala/get_competitor_data.csv")
    
    competitor_data2.printSchema()


//my_data.printSchema()

    

  }
  
}
