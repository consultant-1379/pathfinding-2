package com.ericsson.eniq.bulkcmhandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.event.KafkaEvent;

import com.ericsson.eniq.bulkcmhandler.service.BulkcmProducer;

/*@SpringBootApplication
public class BulkcmHandlerApplication{
	public static void main(String[] args) {
		SpringApplication.run(BulkcmHandlerApplication.class, args);
	}
}*/


@SpringBootApplication(scanBasePackages={"com.ericsson.eniq.bulkcmhandler","com.ericsson.eniq.bulkcmhandler.kafkaConfig","com.ericsson.eniq.bulkcmhandler.controller","com.ericsson.eniq.bulkcmhandler.service"})
public class BulkcmHandlerApplication implements ApplicationListener<KafkaEvent>{

	@Autowired
	private Environment env;
	
	public static void main(String[] args) {
		SpringApplication.run(BulkcmHandlerApplication.class, args);
	}
	@Autowired
	BulkcmProducer bulkcmProduceMessage;
	
	@Override
	public void onApplicationEvent(KafkaEvent event) {
		System.out.println(event);
		
	}
	
	@Bean
	public ApplicationRunner runner(KafkaListenerEndpointRegistry registry,
			 KafkaTemplate<String, String> template) {
		return args -> {
			System.out.println("PAUSING THE CONSUMER");
			System.out.println("From Application handler :"+bulkcmProduceMessage);
			registry.getListenerContainer("bulkcmgroup").pause();
			
	    	registry.getListenerContainer("bulkcmgroup").resume();
	    	System.out.println("CONSUMER RESUMED");
		};
		
	}

}


