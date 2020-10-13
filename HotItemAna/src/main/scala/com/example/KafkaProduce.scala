package com.example

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProduce {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitem")
  }

  def writeToKafka(topic: String): Unit = {
    val porperites = new Properties()
    porperites.setProperty("bootstrap.servers", "192.168.31.202:9092")
    porperites.setProperty("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    porperites.setProperty("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](porperites)
    //从文件读取数据
    val bufferSorce = io.Source.fromFile("C:\\Users\\Administrator.SC-202009132113\\IdeaProjects\\UserExample\\HotItemAna\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferSorce.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()

  }
}
