//
//package main.practice
//import org.apache.spark.sql.{SparkSession, functions}
//import org.apache.spark.sql.functions.col
//
//object kafkaConsumerExample {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder
//      .appName("Kafka Spark Consumer")
//      .master("local[*]") // Adjust this based on your cluster setup
//      .getOrCreate()
//
//    val kafkaDF = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("subscribe", "myTopic")
//      .load()
//
////    kafkaDF.show()
////    kafkaDF.printSchema()
//
//    val messageDF = kafkaDF.selectExpr("CAST(value AS STRING)")
//    val formattedDF = messageDF
//      .withColumn("split_value", functions.split(col("value"), ",")) // Split the string by comma
//      .select(
//        col("split_value").getItem(0).cast("int").as("id"), // First field as id (Int)
//        col("split_value").getItem(1).as("name"), // Second field as name (String)
//        col("split_value").getItem(2).cast("int").as("age"), // Third field as age (Int)
//        col("split_value").getItem(3).cast("decimal(10,2)").as("salary") // Fourth field as salary (Decimal)
//      )
//    formattedDF.show();
//    // Display the messages
//    messageDF.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//      .awaitTermination()
//  }
//}
package main.practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object kafkaConsumerExample {

  // Declare the DataFrame as a global variable
  var formattedDF: DataFrame = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Kafka Spark Consumer")
      .master("local[*]") // Adjust this based on your cluster setup
      .getOrCreate()

    import spark.implicits._

    // Read from Kafka as a streaming DataFrame
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "myTopic")
      .load()

    // Extract the value (message) from the Kafka data as a STRING
    val messageDF = kafkaDF.selectExpr("CAST(value AS STRING)")

    // Split the incoming message by comma and create separate columns
    formattedDF = messageDF
      .withColumn("split_value", split(col("value"), ",")) // Split the string by comma
      .select(
        col("split_value").getItem(0).cast("int").as("id"),       // First field as id (Int)
        col("split_value").getItem(1).as("name"),                 // Second field as name (String)
        col("split_value").getItem(2).cast("int").as("age"),      // Third field as age (Int)
        col("split_value").getItem(3).cast("decimal(10,2)").as("salary") // Fourth field as salary (Decimal)
      )

    // Output the DataFrame to the console in a streaming fashion
    formattedDF.writeStream
      .outputMode("append")   // Since new data will keep arriving, use "append" mode
      .format("console")      // Output to the console
      .start()                // Start the streaming query
      .awaitTermination()     // Keep the query running until terminated
  }
}
