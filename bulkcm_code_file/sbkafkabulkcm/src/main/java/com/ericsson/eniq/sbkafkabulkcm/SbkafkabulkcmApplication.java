package com.ericsson.eniq.sbkafkabulkcm;

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

import com.ericsson.eniq.parser.cache.DBLookupCache;
import com.ericsson.eniq.sbkafkabulkcm.cache.DataFormatCacheImpl;
import com.ericsson.eniq.sbkafkabulkcm.cache.TransformerCacheImpl;

@SpringBootApplication
public class SbkafkabulkcmApplication implements ApplicationListener<KafkaEvent>{

	@Autowired
	private Environment env;
	
	public static void main(String[] args) {
		SpringApplication.run(SbkafkabulkcmApplication.class, args);
	}
	@Override
	public void onApplicationEvent(KafkaEvent event) {
		System.out.println(event);
		
	}
	
	@Bean
	public ApplicationRunner runner(KafkaListenerEndpointRegistry registry,
			 KafkaTemplate<String, String> template) {
		return args -> {
			System.out.println("PAUSING THE CONSUMER");
			registry.getListenerContainer("bulkcmgroup").pause();
			String repdbUrl = env.getProperty("db.repdb.url");
			String dwhdbUrl = env.getProperty("db.dwhdb.url");
			String driver = env.getProperty("db.driver");
			String etlrepUser = env.getProperty("db.repdb.etlrep.user");
			String etlrepPass = env.getProperty("db.repdb.etlrep.pass");
			String dwhrepUser = env.getProperty("db.repdb.dwhrep.user");
			String dwhrepPass = env.getProperty("db.repdb.dwhrep.pass");
			String dwhdbUser = env.getProperty("db.dwhdb.user");
			String dwhdbPass = env.getProperty("db.dwhdb.pass");
	    	DBLookupCache.initialize(driver, 
	    			dwhdbUrl, dwhdbUser, dwhdbPass);
	    	
	    	TransformerCacheImpl dbread = new TransformerCacheImpl();
	    	dbread.readDB(repdbUrl, 
	    			dwhrepUser, dwhrepPass, driver,"dwhrep", "DC_E_BULK_CM");
	    	
	    	DataFormatCacheImpl dataformats = new DataFormatCacheImpl();
	    	dataformats.readDB(repdbUrl, 
	    			dwhrepUser, dwhrepPass, driver, "DC_E_BULK_CM");
	    	System.out.println("From sbkafka bulkcm");
	    	System.out.println("START UP SQUENCE COMPLETE!");
	    	registry.getListenerContainer("bulkcmgroup").resume();
	    	System.out.println("CONSUMER RESUMED");
		};
		
	}
}
