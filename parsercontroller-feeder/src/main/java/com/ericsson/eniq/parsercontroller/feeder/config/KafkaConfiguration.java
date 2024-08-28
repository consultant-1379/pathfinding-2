package com.ericsson.eniq.parsercontroller.feeder.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties.AckMode;


@Configuration
@EnableKafka
public class KafkaConfiguration {

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
		
	@Value("${spring.kafka.consumer.max-poll-records}")
	private Integer maxPollRecords;
	
	@Value("${spring.kafka.consumer.enable-auto-commit}")
	private Boolean isAutoCommit;
	
	@Value("${spring.kafka.consumer.group-id}")
	private String groupId;
	
	@Value("${spring.kafka.consumer.auto-offset-reset}")
	private String autoOffsetReset;
	
	@Value("${spring.kafka.producer.key-serializer}")
	private String keySerializer;
	
	@Value("${spring.kafka.producer.value-serializer}")
	private String valueSerializer;
	
	@Value("${spring.kafka.consumer.key-deserializer}")
	private String keyDeSerializer;
	
	@Value("${spring.kafka.consumer.value-deserializer}")
	private String valueDeSerializer;
		
	private static final Logger LOG = LogManager.getLogger(KafkaConfiguration.class);

	@Bean
	public ConsumerFactory<String, String> consumerFactory(){
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isAutoCommit);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,maxPollRecords);
		LOG.info("Consumer custom configuration = {} ",props);
		return new DefaultKafkaConsumerFactory<>(props);
	}


	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		factory.setBatchListener(true);
		return factory;
	}
	
	@Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,valueSerializer);
        LOG.info("Producer custom configuration = {} ",configProps);
        return new DefaultKafkaProducerFactory<>(configProps);
    }
	
	@Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
	
}
