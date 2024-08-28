package com.ericsson.eniq.parsercontroller;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import com.ericsson.eniq.parsercontroller.service.PCKafkaListener;

@SpringBootTest
//@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" }, topics = ParsercontrollerApplicationTests.TOPIC)
class ParsercontrollerApplicationTests {

	@Test
	void contextLoads() {
	}
	
	//@Autowired
	//PCKafkaListener kafkaListener;
	
	/*private static Consumer<String, String> consumer;
	
	private static KafkaTemplate<String, String> template;
	
	private static final String INPUT_FILE = "/a/b/c/TestFile.xml";
	
	public static final String TOPIC = "PM_E_ERBS_FILES";
	
		
	@BeforeAll
	static void setup() {
		System.out.println("creating setup");
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		System.out.println("embedded kafka : "+embeddedKafka);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		System.out.println("senderProps : "+senderProps);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		AtomicBoolean ppCalled = new AtomicBoolean();
		pf.addPostProcessor(prod -> {
			ppCalled.set(true);
			return prod;
		});
		template = new KafkaTemplate<>(pf, true);
		Map<String, Object> consumerProps = KafkaTestUtils
				.consumerProps("KafkaTemplatetests" + UUID.randomUUID().toString(), "false", embeddedKafka);
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, TOPIC);
				
	}
	
	@Test
	void testListener() {
		System.out.println("sending message");
		template.send(TOPIC, INPUT_FILE);
		PCKafkaListener listener = new PCKafkaListener(null);
		listener.handleRecords(KafkaTestUtils.getRecords(consumer));
	}*/
	
	

}
