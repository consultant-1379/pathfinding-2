package com.ericsson.eniq.bulkcmhandler.controller;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.ericsson.eniq.bulkcmhandler.service.BulkcmProducer;
import com.ericsson.eniq.parser.SourceFile;

@Service
public class BulkcmSplitter {
	
	@Autowired
	private BulkcmProducer producerMessage;
	
	private SourceFile sf;
	private Logger logger = LogManager.getLogger(BulkcmSplitter.class);
	
	private String DELIMITER = " : ";
	Boolean flag = false;
	StringBuilder sb = new StringBuilder();
	
	public void bulkData(final SourceFile sf) throws Exception {
		this.sf = sf;
		final long start = System.currentTimeMillis();
	String eachLine = null;
	try(BufferedReader reader = new BufferedReader(new InputStreamReader(sf.getFileInputStream()))) {
	   System.out.println("File Name is :"+sf.getName());
		while ((eachLine = reader.readLine()) != null ) {
	    	processData(eachLine);
	    }
	    final long end = System.currentTimeMillis();
	    System.out.println("Time taken : "+(end - start)+"ms");
	} catch (IOException x) {
		logger.log(Level.WARN, "IOException while reading file." , x);
	}
	}
	private void processData(String nextLine) {
		Scanner lineScanner = new Scanner(nextLine);
		lineScanner.useDelimiter(DELIMITER);
		if (lineScanner.hasNext()) {
			flag = true;
			sb.append(nextLine+"\n");
		} 
		if(nextLine.length() == 0 &&  nextLine.isEmpty() && flag) {
			StringBuilder sb1 = new StringBuilder();
			sb1.append(sb);
			/*
			String path = "/home/indir/bulkcmhandler/"+ Math.random()+".txt";
			File f =new File(path);
			System.out.println("file name :" +f.getName());
			try {
				FileOutputStream fos = new FileOutputStream(f);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
				
			bw.write(sb1.toString());
			bw.newLine();
				
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
			
			
		// Call bulkcm kafka producser topic
			producerMessage.producerBulkcmMessages("bulkcmOut", sb1.toString());
			flag = false;
			sb = new StringBuilder();
		}
		lineScanner.close();
	}
	
}
