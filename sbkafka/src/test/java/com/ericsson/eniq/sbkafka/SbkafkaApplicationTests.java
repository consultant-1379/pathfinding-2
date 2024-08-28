package com.ericsson.eniq.sbkafka;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import com.ericsson.eniq.sbkafka.config.KafkaTestConfig;


//@Import(KafkaTestConfig.class)
@ContextConfiguration(classes = KafkaTestConfig.class)
@SpringBootTest(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}","spring.main.allow-bean-definition-overriding=true"})
@DirtiesContext
@EmbeddedKafka(topics = "${spring.kafka.consumer.topic}")
class SbkafkaApplicationTests {

	@Value("${spring.kafka.consumer.topic}")
	private String consumerTopic;

	@Value("${producer.topic}")
	private String producerTopic;

	@Value("${spring.embedded.kafka.brokers}")
	private String bootStrapServers;

	@Value("${consumer.id}")
	private String consumerId;

	private static final Logger LOG = LogManager.getLogger(SbkafkaApplicationTests.class);

	//@Autowired
	//private SbKafkaListener sbKafkaListener;

	@Autowired
	@Qualifier("testTemplate")
	public KafkaTemplate<String, String> template;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	// @Test
	// void contextLoads() {
	// }
	
	/*private BatchAcknowledgingMessageListener<Object, Object> messageListener() {
        return (data, acknowledgment) -> {
        	//Consumer records handling logic
        };
    }*/

	@Test
	public void givenEmbeddedKafkaBroker_whenSendingtoDefaultTemplate_thenMessageReceived() throws Exception {
		//ISink sink = spy(ISink.class);
		//SourceFile sf = spy(SourceFile.class);
		//Parser parser = mock(Parser.class).
		//Parser parser = spy(new Parser(sf, "mdc_worker", sink));
		//when(parser.call()).thenReturn(true);
		ConcurrentMessageListenerContainer<?, ?> container = (ConcurrentMessageListenerContainer<?, ?>) registry
				.getListenerContainer(consumerId);
		container.stop();
		//AcknowledgingConsumerAwareMessageListener
		//BatchMessagingMessageListenerAdapter
		@SuppressWarnings("unchecked")
		BatchAcknowledgingConsumerAwareMessageListener<String, String> messageListener = (BatchAcknowledgingConsumerAwareMessageListener<String, String>) container
				.getContainerProperties().getMessageListener();
		CountDownLatch latch = new CountDownLatch(1);
		container.getContainerProperties()
				.setMessageListener(new BatchAcknowledgingConsumerAwareMessageListener<String, String>() {

					@Override
					public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, Acknowledgment acknowledgment,
							Consumer<?, ?> consumer) {
						for (ConsumerRecord<String, String> cr : consumerRecords) {
							System.out.println("received data = "+cr.value());
						}
						//messageListener.onMessage(consumerRecords, acknowledgment, consumer);
						latch.countDown();
					}

				});
		container.start();
		LOG.log(Level.INFO, "Sending with default template");
		System.out.println("Sending with default template");
		template.send(consumerTopic, "Sending with default template");
	}

	/*
	 * @Value("${spring.kafka.consumer.topic}") private String inTopic;
	 * 
	 * @Value("${producer.topic}") private String outTopic;
	 * 
	 * private String inData =
	 * "H:\\WDP\\spring_workspace\\support_files\\erbs_files\\short_file\\A20201104.0715+0000-0730+0000_SubNetwork=ONRM_ROOT_MO,SubNetwork=ERBS-SUBNW-1,MeContext=ERBS1062_statsfileshortfile.xml";
	 * 
	 * private static KafkaTemplate<String, String> template;
	 * 
	 * private static Consumer<String, String> consumer;
	 * 
	 * @BeforeAll public void setUp() { EmbeddedKafkaBroker embeddedKafka =
	 * EmbeddedKafkaCondition.getBroker(); Map<String, Object> senderProps =
	 * KafkaTestUtils.producerProps(embeddedKafka);
	 * DefaultKafkaProducerFactory<String, String> pf = new
	 * DefaultKafkaProducerFactory<>(senderProps); AtomicBoolean ppCalled = new
	 * AtomicBoolean(); pf.addPostProcessor(prod -> { ppCalled.set(true); return
	 * prod; }); template = new KafkaTemplate<>(pf, true); Map<String, Object>
	 * consumerProps = KafkaTestUtils .consumerProps("KafkaTemplatetests" +
	 * UUID.randomUUID().toString(), "false", embeddedKafka);
	 * DefaultKafkaConsumerFactory<String, String> cf = new
	 * DefaultKafkaConsumerFactory<>(consumerProps); consumer = cf.createConsumer();
	 * embeddedKafka.consumeFromAnEmbeddedTopic(consumer, inTopic);
	 * 
	 * // data load
	 * 
	 * 
	 * db.repdb.url=jdbc:sybase:Tds:10.36.255.71:2641
	 * #db.repdb.url=jdbc:postgresql://10.45.193.129:30007/postgres
	 * db.dwhdb.url=jdbc:sybase:Tds:10.36.255.71:2640
	 * db.dwhdb.driver=com.sybase.jdbc4.jdbc.SybDriver
	 * db.repdb.driver=com.sybase.jdbc4.jdbc.SybDriver
	 * #db.repdb.driver=org.postgresql.Driver db.repdb.etlrep.user=etlrep
	 * db.repdb.etlrep.pass=etlrep db.repdb.dwhrep.user=dwhrep
	 * db.repdb.dwhrep.pass=dwhrep db.dwhdb.user=dc
	 * 
	 * 
	 * try { DriverManager.registerDriver(new org.postgresql.Driver());
	 * DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver()); } catch
	 * (SQLException e) { // TODO Auto-generated catch block e.printStackTrace(); }
	 * DBLookupCache.initialize("com.sybase.jdbc4.jdbc.SybDriver",
	 * "jdbc:sybase:Tds:10.36.255.71:2640", "dc", "Dc12#");
	 * 
	 * TransformerCacheImpl dbread = new TransformerCacheImpl();
	 * dbread.readDB("jdbc:sybase:Tds:10.36.255.71:2641", "dwhrep", "dwhrep",
	 * "com.sybase.jdbc4.jdbc.SybDriver", "dwhrep", "DC_E_ERBS");
	 * 
	 * DataFormatCacheImpl dataformats = new DataFormatCacheImpl();
	 * dataformats.readDB("jdbc:sybase:Tds:10.36.255.71:2641", "dwhrep", "dwhrep",
	 * "com.sybase.jdbc4.jdbc.SybDriver");
	 * 
	 * }
	 * 
	 * @Test public void testMessage() { template.send(inTopic, inData); }
	 */
}
