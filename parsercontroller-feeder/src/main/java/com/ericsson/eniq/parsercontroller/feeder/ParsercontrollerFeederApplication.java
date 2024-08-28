package com.ericsson.eniq.parsercontroller.feeder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication(scanBasePackages={"com.ericsson.eniq.parsercontroller.feeder","com.ericsson.eniq.parsercontroller.feeder.config","com.ericsson.eniq.parsercontroller.feeder.core","com.ericsson.eniq.parsercontroller.feeder.service"})
public class ParsercontrollerFeederApplication {

	public static void main(String[] args) {
		SpringApplication.run(ParsercontrollerFeederApplication.class, args);
	}

}
