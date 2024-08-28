package com.ericsson.eniq.bulkcmhandler.controller;

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

import com.ericsson.eniq.bulkcmhandler.pojo.ParserInputForm;
import com.ericsson.eniq.bulkcmhandler.pojo.ParserInputFormList;
import com.ericsson.eniq.parser.ExecutionManager;
import com.ericsson.eniq.parser.ParseSession;
import com.ericsson.eniq.parser.SourceFile;

@Controller
public class BulkcmController {

	/*private static final Logger logger = LogManager.getLogger(BulkcmController.class);
	@RequestMapping (value="/split" , method=RequestMethod.POST, consumes="application/json", produces="text/plain")
	@ResponseBody
public void parse(@RequestBody ParserInputFormList inputDataList) throws SQLException {
		
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
		
		BulkcmSplitter bulkcmSplitter = new BulkcmSplitter(sf,techpack, setType, setName);
		try {
		//	bulkcmSplitter.bulkData(sf);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	Parser parser = new Parser(sf, techpack, setType, setName, "worker");
		workers.add(bulkcmSplitter);
	//	workers.add(parser);
		  }
		}
		ExecutionManager.getInstance().addParserToExecution(workers);
	}*/
	
}

