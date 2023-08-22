import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType,IntegerType}
import org.apache.spark.sql.SparkSession


object streamstaticjoin extends App{

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("File Sink")
      .getOrCreate()
  
        println("one")

    // Set Spark logging level to ERROR to avoid various other logs on console.
    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(List(
      StructField("order_id", DoubleType, true),
      StructField("order_date", DoubleType, true),
      StructField("order_customer_id", DoubleType, true),
      StructField("order_status", StringType, true)
      
    ))

     println("two")
    // Create Streaming DataFrame by reading data from socket.
    val read1 = (spark
      .readStream
      .option("maxFilesPerTrigger", 2) // This will read maximum of 2 files per mini batch. However, it can read less than 2 files.
      .option("header", true)
      .schema(schema)
      .format("csv")
      .load("/home/cloudera/Desktop/hemant/orders*.csv")
      )
      println("three")
   
    val read2 = spark.read.option("header",true).csv("/home/cloudera/Desktop/hemant1/orders*.csv")
  
    println("four")
    
 //   read1.show()
    read2.show()
    
    
    val joinDf = read2.join(read1,Seq("order_id"),"Inner")
    
    joinDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

    
    
}