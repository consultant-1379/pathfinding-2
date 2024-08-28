package com.ericsson.eniq.etl.mdc.controller;

import java.io.File;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.distocraft.dc5000.etl.binaryformatter.BinFormatter;
import com.distocraft.dc5000.etl.parser.ParseSession;
import com.distocraft.dc5000.etl.parser.ParserDebugger;
import com.distocraft.dc5000.etl.parser.ParserDebuggerCache;
import com.distocraft.dc5000.etl.parser.SourceFile;
import com.distocraft.dc5000.etl.parser.TransformerCache;
import com.distocraft.dc5000.repository.cache.DBLookupCache;
import com.distocraft.dc5000.repository.cache.DFormat;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.ericsson.eniq.etl.utils.ParserInput;
import com.ericsson.eniq.etl.mdc.parser.MDCParser;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

@Controller
public class MdcApplicationController {
	
	private static final String REP_DB_URL_JCONNECT= "jdbc:sybase:Tds:10.45.199.105:2641";
	private static final String DWH_DB_URL_JCONNECT= "jdbc:sybase:Tds:10.45.199.105:2640";
	//private static final String DB_URL ="jdbc:sqlanywhere:host=10.45.199.105:2641;InitString='SET TEMPORARY OPTION CONNECTION_AUTHENTICATION=''Company=Ericsson;Application=ENIQ;Signature=000fa55157edb8e14d818eb4fe3db41447146f1571g539f0a8f80fd6239ea117b9d74be36c19c58dc14'''";
	private static final String DWHREP_USERNAME = "dwhrep";
	private static final String DWHREP_PASSWORD = "dwhrep";
	private static final String DWHDB_USERNAME = "dc";
	private static final String DWHDB_PASSWORD = "dc";
	private static final String ETL_USERNAME = "etlrep";
	private static final String ETL_PASSWORD = "etlrep";
	private static final String DB_DRIVERNAME_JCONNECT = "com.sybase.jdbc4.jdbc.SybDriver";
	//private static final String DB_DRIVERNAME = "sap.jdbc4.sqlanywhere.IDriver";
	private static final Logger LOGGER = Logger.getLogger("mdc_Logger");
	
	@RequestMapping (value="/parse" , method=RequestMethod.POST, consumes="application/json", produces="text/plain")
	@ResponseBody
	public String parse(@RequestBody ParserInput input) throws SQLException {
		RockFactory etlRf = null;
		System.out.println("Input for Parser : "+input.toString());
		Map<String, String> actionContents = input.getActionContents();
		try {
			etlRf = new RockFactory(REP_DB_URL_JCONNECT, ETL_USERNAME, ETL_PASSWORD, DB_DRIVERNAME_JCONNECT, "Mdc_Sf", false);
			File inputFile = new File(input.getInputFile());
			String setName = input.getSetName();
			String setType = input.getSetType();
			int mc = 1000;
			String tp = input.getTp();
			ParserDebugger debugger = new ParserDebuggerCache();
			System.out.println( " Conf loaded = "+ actionContents);
			if (!actionContents.isEmpty()) {
				Properties conf = new Properties();
				conf.putAll(actionContents);
				ParseSession psession = new ParseSession(8888, conf);
				SourceFile sf = new SourceFile(inputFile, mc, conf , etlRf, psession, debugger,
						conf.getProperty("useZip", "gzip"), LOGGER);
				MDCParser parser = new MDCParser(sf,tp,setType,setName,"mdc_worker");
				ExecutorService ex = Executors.newFixedThreadPool(1);
				ex.execute(parser);
			} else {
				System.out.println("Configuration is empty");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (etlRf != null && etlRf.getConnection() != null) {
				
				etlRf.getConnection().close();
			}
		}
		return actionContents.toString();
	}
	
	@RequestMapping (value="/init" , method=RequestMethod.GET)
	@ResponseBody
	public String init() {
		System.out.println("InitializingCache ");
		try {
			initializeCache();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Cache Initialized ");
		return "Cache Initialized";
	}
	
	@RequestMapping (value="/test" , method=RequestMethod.GET)
	@ResponseBody
	public String test() {
		return "mdcparser: Hello";
	}
	
	public static void initializeCache() throws SQLException {
		RockFactory dwhRf = null;
		RockFactory etlRf = null;
		
		try {
			//System.setProperty("java.library.path", "/eniq/sybase_iq/IQ-16_1/lib64");
			//System.setProperty("java.library.path", "/eniq/home/dcuser/logesh/mdc/sap_lib");
			dwhRf = new RockFactory(REP_DB_URL_JCONNECT, DWHREP_USERNAME, DWHREP_PASSWORD, DB_DRIVERNAME_JCONNECT, "mdc_Cache", false);
			etlRf = new RockFactory(REP_DB_URL_JCONNECT, ETL_USERNAME, ETL_PASSWORD, DB_DRIVERNAME_JCONNECT, "mdc_Cache", false);
			BinFormatter.createClasses();
			DBLookupCache.initialize(DB_DRIVERNAME_JCONNECT, DWH_DB_URL_JCONNECT, DWHDB_USERNAME, DWHDB_PASSWORD);
			TransformerCache tc = new TransformerCache();
			TransformerCache.getCache().revalidate(dwhRf, etlRf);
			DataFormatCache.initialize(DB_DRIVERNAME_JCONNECT, REP_DB_URL_JCONNECT, DWHREP_USERNAME, DWHREP_PASSWORD);
			
			DFormat df = DataFormatCache.getCache().getFormatWithTagID("INTF_DC_E_MGW", "EtResource");
			System.out.println("FolderName = " + df.getFolderName() + " : DataItems = "+df.getDitems());
			System.out.println("Transformation cache details : "+tc.getTransformer("DC_E_MGW:((131)):DC_E_MGW_AAL2AP:mdc").getTransformations());
		} catch (SQLException | RockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (dwhRf != null) {
				dwhRf.getConnection().close();
			}
			if (etlRf != null) {
				etlRf.getConnection().close();
			}
		}
	}

}
