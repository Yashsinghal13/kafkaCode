package main.practice

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

object kafkaProducerExample {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send messages to Kafka topic
    for (i <- 5 to 10) {
      val record = new ProducerRecord[String, String]("myTopic", s"key_$i", s"value_$i")
      producer.send(record)
    }
    producer.close()
    println("Data send to producer successfully")
  }
}
