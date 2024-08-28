package com.ericsson.eniq.etl.g2_rest.controller;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ericsson.eniq.etl.g2_rest.pojo.ParserInputForm;
import com.ericsson.eniq.etl.g2_rest.pojo.ParserInputList;
import com.ericsson.eniq.parser.ExecutionManager;
import com.ericsson.eniq.parser.ParseSession;
import com.ericsson.eniq.parser.SourceFile;
import com.ericsson.eniq.parser.GPP32435Parser.Parser;

@Controller
public class Parser3gppController {
	
private static final Logger logger = LogManager.getLogger(Parser3gppController.class);
	
	@RequestMapping (value="/parse" , method=RequestMethod.POST, consumes="application/json", produces="text/plain")
	@ResponseBody
	public void parse(@RequestBody ParserInputList inputDataList) throws SQLException {
		
		List<Callable<Boolean>> workers = new ArrayList<>();
		List<ParserInputForm> list = inputDataList.getParserList();
		if (list != null) {
		for(ParserInputForm inputData : list) {
		File inputFile = new File(inputData.getInputFile());
		String setName = inputData.getSetName();
		String setType = inputData.getSetType();
		String techpack = inputData.getTp();
		Properties conf = new Properties();
		conf.putAll(inputData.getActionContents());
		
		ParseSession session = new ParseSession(8888, conf);
		SourceFile sf = new SourceFile(inputFile, conf, session,
				conf.getProperty("useZip", "gzip"), logger);
		
		Parser parser = new Parser(sf, techpack, setType, setName, "3gpp_worker");
		workers.add(parser);
		  }
		}
		ExecutionManager.getInstance().addParserToExecution(workers);
	}

	@RequestMapping (value="/test" , method=RequestMethod.GET)
	@ResponseBody
	public String test() {
		return "3GPPparser: Hello";
	}
}
