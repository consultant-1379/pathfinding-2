package com.ericsson.eniq.sb_rest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class ApplicationMain {
	
	
	
    public static void main( String[] args ){   
    	SpringApplication.run(ApplicationMain.class, args);
    	 
//    	try {
//			System.out.println("DATAFORMATS");
//			DataFormatCache.getCache().listDataFormats();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
    }
}
