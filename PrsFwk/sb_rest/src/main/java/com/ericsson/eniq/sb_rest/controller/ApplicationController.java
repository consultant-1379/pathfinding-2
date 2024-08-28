package com.ericsson.eniq.sb_rest.controller;

import java.io.File;
import java.lang.reflect.Type;
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

import com.ericsson.eniq.parser.ExecutionManager;
import com.ericsson.eniq.parser.ParseSession;
import com.ericsson.eniq.parser.Parser;
import com.ericsson.eniq.parser.SourceFile;
import com.ericsson.eniq.sb_rest.controller.pojo.ParserInput;
import com.ericsson.eniq.sb_rest.controller.pojo.ParserInputList;
import com.google.gson.Gson;

@Controller
public class ApplicationController {
	
	private static final Logger logger = LogManager.getLogger(ApplicationController.class);
	
	@RequestMapping (value="/parse" , method=RequestMethod.POST, consumes="application/json", produces="text/plain")
	@ResponseBody
	public void parse(@RequestBody ParserInputList inputList) throws SQLException {
		try {
			
			List<Callable<Boolean>> workers = new ArrayList<>();
			List<ParserInput> list = inputList.getParserList();
			if (list != null) {
				for (ParserInput input : list) {
					File inputFile = new File(input.getInputFile());
					String setName = input.getSetName();
					String setType = input.getSetType();
					String techpack = input.getTp();
					Properties conf = new Properties();
					conf.putAll(input.getActionContents());
					
					ParseSession session = new ParseSession(8888, conf);
					SourceFile sf = new SourceFile(inputFile, conf, session,
							conf.getProperty("useZip", "gzip"), logger);
					
					Parser parser = new Parser(sf, techpack, setType, setName, "mdc_worker");
					workers.add(parser);
				}
			}
				
			ExecutionManager.getInstance().addParserToExecution(workers);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	@RequestMapping (value="/test" , method=RequestMethod.GET)
	@ResponseBody
	public String test() {
		return "mdcparser: Hello";
	}

}
