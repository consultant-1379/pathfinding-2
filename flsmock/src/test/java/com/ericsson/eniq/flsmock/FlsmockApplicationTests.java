package com.ericsson.eniq.flsmock;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.ericsson.eniq.flsmock.controller.FlsmockController;
import com.ericsson.eniq.flsmock.pojo.ProducerInput;
import com.ericsson.eniq.flsmock.service.Producer;

//@SpringBootTest

@EmbeddedKafka(topics = FlsmockApplicationTests.TOPIC )
class FlsmockApplicationTests {
	
	private static KafkaTemplate<String, String> template;
	
	private static Consumer<String, String> consumer;
	
	
	private static final String INPUT_FILE = "/a/b/c/TestFile.xml";
	
	public static final String TOPIC = "PM_E_ERBS_FILES";
	
	
	
	
	
	//@Test
	//void contextLoads() {
	//}
	
	
	@BeforeAll
	public static void setUp() {
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		System.out.println("embeddedKafka = " + embeddedKafka);
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		System.out.println("senderPropes = " + senderProps);
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
	
	@AfterAll
	static void tearDown() {
		consumer.close();
	}
	
	@Test
	void testController() {
		
	    ProducerInput input = new ProducerInput();
	    input.setInputFile(INPUT_FILE);
	    input.setTopic(TOPIC);
	    input.setMessagesPerBatch(1);
	    input.setRepeat(true);
	    input.setTotalBatches(1);
	    FlsmockController controller = new FlsmockController(new Producer(template));
	    controller.produceMessages(input);
	    assertEquals(KafkaTestUtils.getSingleRecord(consumer, TOPIC).value(),INPUT_FILE);
		
	}
	
	
	

}
