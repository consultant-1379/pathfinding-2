package com.ericsson.eniq.sbkafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;

@Configuration
@EnableKafka
public class ProducerConfiguration {
	
	@Value("${schema.registry.url}") 
	private String schemaRegistryUrl;
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
	
	@Bean
    public ProducerFactory<String, GenericRecord> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
          ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //configProps.put(ProducerConfig.LINGER_MS_CONFIG,25);
        //configProps.put(ProducerConfig.BATCH_SIZE_CONFIG,100000);
        //configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,KafkaAvroSerializer.class);
        configProps.put("schema.registry.url", schemaRegistryUrl);
        configProps.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
        //configProps.put("auto.register.schemas", false);
        //configProps.put("use.latest.version", true);
        //configProps.put("schema.compatibility.level", "none");
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    //@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public KafkaTemplate<String, GenericRecord> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
    
    
    
    

}
