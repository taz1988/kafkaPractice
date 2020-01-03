/*
 * Copyright (c) 2011-2020, Hortonworks Inc. All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package com.practice.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemo.class);

  public static void main(String[] args) {
    KafkaProducer<String, String> producer = createProducer();



    Callback callback = createCallback();

    for (int i = 0; i < 10; i++) {
      ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test","Key " + i, "hello world! " + i);
      producer.send(producerRecord, callback);
    }

    producer.close();
  }

  private static Callback createCallback() {
    return new Callback() {
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          LOGGER.info("Got metadata, metaData=" + recordMetadata.toString());
      }
    };
  }

  private static KafkaProducer<String, String> createProducer()
  {
    Properties properties = producerProperties();
    return new KafkaProducer<String, String>(properties);
  }

  private static Properties producerProperties() {

    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return properties;
  }
}
