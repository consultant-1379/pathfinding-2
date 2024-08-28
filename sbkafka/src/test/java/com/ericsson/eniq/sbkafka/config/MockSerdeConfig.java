/*package com.ericsson.eniq.sbkafka.config;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Configuration
class MockSerdeConfig {
   // KafkaProperties groups all properties prefixed with `spring.kafka`
   private KafkaProperties props;
   
   MockSerdeConfig(KafkaProperties kafkaProperties) {
      props = kafkaProperties;
   }

   *//**
    * Mock schema registry bean used by Kafka Avro Serde since
    * the @EmbeddedKafka setup doesn't include a schema registry.
    * @return MockSchemaRegistryClient instance
    *//*
   @Bean
   MockSchemaRegistryClient schemaRegistryClient() {
      return new MockSchemaRegistryClient();
   }

   *//**
    * KafkaAvroSerializer that uses the MockSchemaRegistryClient
    * @return KafkaAvroSerializer instance
    *//*
   @Bean
   KafkaAvroSerializer kafkaAvroSerializer() {
      return new KafkaAvroSerializer(schemaRegistryClient());
   }

   *//**
    * KafkaAvroDeserializer that uses the MockSchemaRegistryClient.
    * The props must be provided so that specific.avro.reader: true
    * is set. Without this, the consumer will receive GenericData records.
    * @return KafkaAvroDeserializer instance
    *//*
   @Bean
   KafkaAvroDeserializer kafkaAvroDeserializer() {
      return new KafkaAvroDeserializer(schemaRegistryClient(), props.buildConsumerProperties());
   }

   *//**
    * Configures the kafka producer factory to use the overridden
    * KafkaAvroDeserializer so that the MockSchemaRegistryClient
    * is used rather than trying to reach out via HTTP to a schema registry
    * @return DefaultKafkaProducerFactory instance
    *//*
   @Bean
   DefaultKafkaProducerFactory<String, Object> producerFactory() {
     return new DefaultKafkaProducerFactory<>(
            props.buildProducerProperties(),
            new StringSerializer(),
            kafkaAvroSerializer()
      );
   }

   *//**
    * Configures the kafka consumer factory to use the overridden
    * KafkaAvroSerializer so that the MockSchemaRegistryClient
    * is used rather than trying to reach out via HTTP to a schema registry
    * @return DefaultKafkaConsumerFactory instance
    *//*
   @Bean
   DefaultKafkaConsumerFactory<String, Object> consumerFactory() {
      return new DefaultKafkaConsumerFactory<>(
            props.buildConsumerProperties(),
            new StringDeserializer(),
            kafkaAvroDeserializer()
      );
   }

   *//**
    * Configure the ListenerContainerFactory to use the overridden
    * consumer factory so that the MockSchemaRegistryClient is used
    * under the covers by all consumers when deserializing Avro data.
    * @return ConcurrentKafkaListenerContainerFactory instance
    *//*
   @Bean
   ConcurrentKafkaListenerContainerFactory<String, GenericRecord> kafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory());
      return factory;
   }
}
*/