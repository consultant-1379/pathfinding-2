package com.ericsson.eniq.sbkafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@TestConfiguration
public class KafkaTestConfig {
	
	@Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;
	
	@Bean
	public Map<String, Object> testConsumerConfigs() {
		Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("groupId", "false", embeddedKafkaBroker));
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return configs;
	}

	@Bean
	public ConsumerFactory<String, String> testConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(testConsumerConfigs(),new StringDeserializer(), new StringDeserializer());
	}

	@Bean
	@Primary
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(testConsumerFactory());
		factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		factory.setBatchListener(true);
		return factory;
	}
	
	@Bean
	public Map<String, Object> testProducerConfigs() {
		return new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
	}
	
	
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(testProducerConfigs(), new StringSerializer(), new StringSerializer());
	}

	@Bean(name = "testTemplate")
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

}
