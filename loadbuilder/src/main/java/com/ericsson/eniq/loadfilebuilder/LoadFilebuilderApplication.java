package com.ericsson.eniq.loadfilebuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ericsson.eniq.loadfilebuilder.cache.TableNameCache;

@SpringBootApplication(scanBasePackages={"com.ericsson.eniq.loadfilebuilder","com.ericsson.eniq.loadfilebuilder.cache","com.ericsson.eniq.loadfilebuilder.config","com.ericsson.eniq.loadfilebuilder.controller", "com.ericsson.eniq.loadfilebuilder.outputstream"})
public class LoadFilebuilderApplication implements ApplicationRunner {

	@Autowired
	TableNameCache cache;
	
	public static void main(String[] args) {
		SpringApplication.run(LoadFilebuilderApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		cache.initCache();
	}

}
