package com.ericsson.eniq.sb_rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import com.ericsson.eniq.parser.cache.DBLookupCache;
import com.ericsson.eniq.sb_rest.cache.DataFormatCacheImpl;
import com.ericsson.eniq.sb_rest.cache.TransformerCacheImpl;


@Component
public class CacheLoader implements ApplicationRunner{
	
	@Autowired
	private Environment env;
		
	String transId;

	@Override
	public void run(ApplicationArguments args) throws Exception {
		initializeCache();
		//runTests();
		
	}
	
	private void initializeCache() {
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
    			dwhrepUser, dwhrepPass, driver,"dwhrep", "DC_E_ERBS");
    	
    	DataFormatCacheImpl dataformats = new DataFormatCacheImpl();
    	dataformats.readDB(repdbUrl, 
    			dwhrepUser, dwhrepPass, driver, "DC_E_ERBS");
    	
    	System.out.println("START UP SQUENCE COMPLETE!");
	}
	
}
