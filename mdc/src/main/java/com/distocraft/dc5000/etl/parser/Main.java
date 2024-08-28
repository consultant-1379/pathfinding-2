package com.distocraft.dc5000.etl.parser;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import ssc.rockfactory.RockFactory;
import com.distocraft.dc5000.common.ProcessedFiles;
import com.distocraft.dc5000.common.SessionHandler;
import com.distocraft.dc5000.common.StaticProperties;
//import com.distocraft.dc5000.etl.engine.common.EngineCom;
//import com.distocraft.dc5000.etl.engine.common.Share;
//import com.distocraft.dc5000.etl.engine.executionslots.ExecutionMemoryConsumption;
//import com.distocraft.dc5000.etl.engine.executionslots.ExecutionSlotProfileHandler;
//import com.distocraft.dc5000.etl.engine.main.EngineThread;
//import com.distocraft.dc5000.etl.engine.priorityqueue.PriorityQueue;
import com.distocraft.dc5000.etl.rock.Meta_databases;
import com.distocraft.dc5000.repository.cache.DFormat;
import com.distocraft.dc5000.repository.cache.DItem;
import com.distocraft.dc5000.repository.cache.DataFormatCache;
import com.ericsson.eniq.common.CommonUtils;
import com.ericsson.eniq.common.RemoteExecutor;
import com.ericsson.eniq.repository.DBUsersGet;
import com.jcraft.jsch.JSchException;

/**
 * <br>
 * <table border="1" width="100%" cellpadding="3" cellspacing="0">
 * <tr bgcolor="#CCCCFF" class="TableHeasingColor">
 * <td colspan="4"><font size="+2"><b>Parameter Summary</b></font></td>
 * </tr>
 * <tr>
 * <td><b>Name</b></td>
 * <td><b>Key</b></td>
 * <td><b>Description</b></td>
 * <td><b>Default</b></td>
 * </tr>
 * <tr>
 * <td>Type</td>
 * <td>parserType</td>
 * <td>Defines specific parser type.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Interface name</td>
 * <td>interfaceName</td>
 * <td>Defines the interface name of the parser.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>inDir</td>
 * <td>Where parser reads (input)datafiles.</td>
 * <td>BaseDir + in/</td>
 * </tr>
 * <tr>
 * <td>Base directory</td>
 * <td>baseDir</td>
 * <td>Where are all the needed directories located: archive,failed,dublicate and processed.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Minumum file age</td>
 * <td>minFileAge</td>
 * <td>Defines the minimum age of the (input)datafile before it is read by the parser.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Loader directory</td>
 * <td>loaderDir</td>
 * <td>Defines the directory where created datafiles are moved (in measurement spesific subdirectories) from output
 * directory.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Max files per run</td>
 * <td>maxFilesPerRun</td>
 * <td>How many files parser reads from the inDir. If the total amount of the files (in inDir) exeeds the limit, extra
 * files are left in inDir.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Output directory</td>
 * <td>outDir</td>
 * <td>Defines where the parser created output files are moved before they are distributet to the <b>loaderDir</b></td>
 * <td>BaseDir + out/</td>
 * </tr>
 * <tr>
 * <td>Archive directory lenght</td>
 * <td>archivePeriod</td>
 * <td>Defines the number of hours that an active archive directory is kept active. Archive directories are:
 * archived,failed and double.</td>
 * <td>168</td>
 * </tr>
 * <tr>
 * <td>Processed list directory</td>
 * <td>processedDir</td>
 * <td>Defines the directory where processed filelist is written.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>Filename format</td>
 * <td>fileNameFormat</td>
 * <td>RegExp pattern that defines which files are read from inDir.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>checkType</td>
 * <td>How is the broken link check done: <br>
 * when listing files:<br>
 * 0=no check, 1=exists, 2=isFile, 3=canRead<br>
 * In nextFile:<br>
 * 0=exists, 1=exists, 2=isFile, 3=canRead</td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>Dublicate check</td>
 * <td>dublicateCheck</td>
 * <td>Defines (true or false) if dublicate check is done (from processed files) to datafiles before parsing.</td>
 * <td>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>After parse action</td>
 * <td>afterParseAction</td>
 * <td>Defines what is done to the parsed datafile: <b>move</b>,<b>delete</b> or <b>no</b>. Move moves the file to
 * archiveDir. Delete deletes the file. No does nothing file stays at the inDir.</td>
 * <td>move</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>archiveStampFormat</td>
 * <td>Defines the format (simpleDateformat) in which timestamp is added to archive directory.</td>
 * <td>yyyyMMddHHmm</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>Parser.tempFileHandlingCase</td>
 * <td>Defines what is done to file found in adater_tmp directory when new parser is started.<br>
 * 0: Does nothing.<br>
 * 1: Deletes all the files.<br>
 * 2: Moves files to loader directory.<br>
 * 3: Deletes the last row of every file.<br>
 * </td>
 * <td>3</td>
 * </tr>
 * <tr>
 * <td>&nbsp;</td>
 * <td>Parser.tempFileHandlingCase</td>
 * <td>Defines what is done to file found in adater_tmp directory when new
 * </table>
 * 
 * 
 * @author lemminkainen
 * @author savinen
 */
public class Main {

	private static final String WORKING_COPY_SUFFIX = "_workingcopy";

	public static final String SESSIONTYPE = "ADAPTER";

	private final Logger log;

	private final Logger flog;

	private final Logger performanceLog;

	private final Properties conf;

	private final String techPack;

	private final String set_type;

	private final String set_name;

	private final String parserType;

	private final RockFactory rf;

	private RockFactory reprock;

	private long totalSize;

	private File archiveDir = null;

	private File doubleDir = null;

	private File failedDir = null;

	private ParserDebugger debugger = null;

	private List<FileInformation> fileList = new ArrayList<FileInformation>();

	private final List<File> localDirLockList = new ArrayList<File>();

	private ProcessedFiles checker = null;

	private ParseSession psession = null;

	private int batchID = 0;

	private int allRows = 0;

	private long parse_start = 0L;

	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	private Set<String> measSet = new HashSet<String>();

	private int fileCount = 0;
	
	private int newFileCount = 0;
	
	private long newFileSize = 0L;

	//private EngineCom eCom = null;

	private int checkType = 0;

	private Parser internalWorker = null;

	protected boolean useMemoryRestriction = false;

	protected int memoryUsageFactor = 0;
	
	long totalfilesize;
	long fileSize ;
	
	private Map<String, SourceFile> zipSourceFiles = new HashMap<>();
	
	protected String regexpForWorkerLimit = "";

	// larges possible filecount for one run.
	private static final int MAXFILECOUNT = 32766;	//EQEV-39001 - Since BATCH_ID value cannot exceed 32767 currently

	private DataFormatCache dfCache = null;

	private String outputFormat = "";

	// For CR 1023/109 18-FCP 103 8147
	private String fileNameFilter = null;

	private final List<SourceFile> sourceFileList;
	
	private final String rollingHideDir;
	private Object sizelock = new Object();
	private String enginerobustflag="";
	
	private Map<String,Integer> zipCountMap;

	/**
	 * Adapter Log Cache
	 */
	private final List<Map<String, Object>> parserSessionLogs = new ArrayList<Map<String, Object>>();

	public int NoOfFileSystem;

	public static int NoOfDir;

	public String preAppend = "0";

	/**
	 * Initializes parser for defined instance.
	 * 
	 * @param instanceName
	 *            name of instance in config-file.
	 * @param techPack
	 *            Technology package name
	 * @param set_type
	 *            Name of the set type
	 * @param set_name
	 *            Name of the set
	 */
	public Main(final Properties conf, final String techPack, final String set_type, final String set_name,
			final RockFactory rf, final RockFactory reprock) throws Exception {

		//this.eCom = eCom;/
		this.conf = conf;
		this.techPack = techPack;
		this.set_type = set_type;
		this.set_name = set_name;
		this.rf = rf;
		this.reprock = reprock;
		this.parserType = conf.getProperty("parserType");
		this.outputFormat = conf.getProperty("outputFormat", null);
		
		log = Logger.getLogger("etl." + techPack + "." + set_type + "." + set_name + ".parser");
		flog = Logger.getLogger("file." + techPack + "." + set_type + "." + set_name + ".parser");
		performanceLog = Logger.getLogger("performance." + techPack + ".parser." + parserType);

		
		if ((null == outputFormat) || (this.outputFormat.equals(""))) {
			// check for Parser Type
			if (parserType.equalsIgnoreCase("MDC")) {
				this.outputFormat = conf.getProperty("MDCParser.outputFormat", "0");
			} else if (parserType.equalsIgnoreCase("3gpp32435")) {
				this.outputFormat = conf.getProperty("x3GPPParser.outputFormat", "0");
			} else {
				// by default ASCII output
				this.outputFormat = "0";
			}
			log.finest("Parser Type is " + parserType.toString());
			log.finest("Output Format is " + outputFormat.toString());
		} else {
			// Check output format
			log.finest("Output Format is " + outputFormat.toString());
		}
		
		//Regd: EQEV-42671 JIRA. As sourceFileList is accessed by all worker threads.
		//So one of the worker thread was failing in adding the file to list resulting in issue.
		sourceFileList =  Collections.synchronizedList(new ArrayList<SourceFile>())  ;

		determineFileFilterNameproperty();

		final String interfaceName = conf.getProperty("interfaceName", null);

		if ((interfaceName == null) || (interfaceName.length() <= 0)) {
			throw new Exception("Parameter interfaceName is not defined");
		}

		//final Share share = Share.instance();

		Map<String, Integer> memoryUsageFactors = new HashMap<String, Integer>();
		Map<String, String> regexpsForWorkerLimitations = new HashMap<String, String>();

		//memoryUsageFactors = (HashMap<String, Integer>) share.get("memory_usage_factors");
		//regexpsForWorkerLimitations = (HashMap<String, String>) share.get("regexps_for_worker_limitations");
		memoryUsageFactors = new HashMap<>();
		regexpsForWorkerLimitations = new HashMap<>();
		useMemoryRestriction = configureMemoryUsageRestrictions(regexpsForWorkerLimitations, memoryUsageFactors);
		
		File rollingUpgradeFile = new File(StaticProperties.getProperty("rollingUpgrade.flagFile", "/tmp/rolling_upgrade"));
		rollingHideDir = StaticProperties.getProperty("rollingUpgrade.hiddenDirName", ".upgrade_hidden");
		//Is rolling upgrade currently happening?
		if (rollingUpgradeFile.exists()){
			log.info("Rolling upgrade is happening - flag found: "+rollingUpgradeFile);
			//Yes, so check if it is an interface that needs to have files hidden during rolling upgrade?
			final List hideFileInterfaces = strToList(StaticProperties.getProperty("rollingUpgrade.hideFileInterfaces",""));
			if (hideFileInterfaces.contains(interfaceName)){ //When another interface is to hide files during rolling upgrade, add it to this conditional statement
				//Files are to be hidden at end of adapter session.
				conf.setProperty("afterParseAction", "hide");
				log.info("Source files will be hidden when processed.");
			}
		}
	}
	
	/**
	 * Initializes parser for defined instance.
	 * 
	 * @param instanceName
	 *            name of instance in config-file.
	 * @param techPack
	 *            Technology package name
	 * @param set_type
	 *            Name of the set type
	 * @param set_name
	 *            Name of the set
	 */
	public Main(final Properties conf, final String techPack, final String set_type, final String set_name,
			final RockFactory rf) throws Exception {

		
		this.conf = conf;
		this.techPack = techPack;
		this.set_type = set_type;
		this.set_name = set_name;
		this.rf = rf;
		this.parserType = conf.getProperty("parserType");
		this.outputFormat = conf.getProperty("outputFormat", null);
		
		log = Logger.getLogger("etl." + techPack + "." + set_type + "." + set_name + ".parser");
		flog = Logger.getLogger("file." + techPack + "." + set_type + "." + set_name + ".parser");
		performanceLog = Logger.getLogger("performance." + techPack + ".parser." + parserType);

		
		if ((null == outputFormat) || (this.outputFormat.equals(""))) {
			// check for Parser Type
			if (parserType.equalsIgnoreCase("MDC")) {
				this.outputFormat = conf.getProperty("MDCParser.outputFormat", "0");
			} else if (parserType.equalsIgnoreCase("3gpp32435")) {
				this.outputFormat = conf.getProperty("x3GPPParser.outputFormat", "0");
			} else {
				// by default ASCII output
				this.outputFormat = "0";
			}
			log.finest("Parser Type is " + parserType.toString());
			log.finest("Output Format is " + outputFormat.toString());
		} else {
			// Check output format
			log.finest("Output Format is " + outputFormat.toString());
		}
		
		//Regd: EQEV-42671 JIRA. As sourceFileList is accessed by all worker threads.
		//So one of the worker thread was failing in adding the file to list resulting in issue.
		sourceFileList =  Collections.synchronizedList(new ArrayList<SourceFile>())  ;

		determineFileFilterNameproperty();

		final String interfaceName = conf.getProperty("interfaceName", null);

		if ((interfaceName == null) || (interfaceName.length() <= 0)) {
			throw new Exception("Parameter interfaceName is not defined");
		}

		Map<String, Integer> memoryUsageFactors = new HashMap<>();
		Map<String, String> regexpsForWorkerLimitations = new HashMap<>();

		//memoryUsageFactors = (HashMap<String, Integer>) share.get("memory_usage_factors");
		//regexpsForWorkerLimitations = (HashMap<String, String>) share.get("regexps_for_worker_limitations");

		useMemoryRestriction = configureMemoryUsageRestrictions(regexpsForWorkerLimitations, memoryUsageFactors);
		
		File rollingUpgradeFile = new File(StaticProperties.getProperty("rollingUpgrade.flagFile", "/tmp/rolling_upgrade"));
		rollingHideDir = StaticProperties.getProperty("rollingUpgrade.hiddenDirName", ".upgrade_hidden");
		//Is rolling upgrade currently happening?
		if (rollingUpgradeFile.exists()){
			log.info("Rolling upgrade is happening - flag found: "+rollingUpgradeFile);
			//Yes, so check if it is an interface that needs to have files hidden during rolling upgrade?
			final List hideFileInterfaces = strToList(StaticProperties.getProperty("rollingUpgrade.hideFileInterfaces",""));
			if (hideFileInterfaces.contains(interfaceName)){ //When another interface is to hide files during rolling upgrade, add it to this conditional statement
				//Files are to be hidden at end of adapter session.
				conf.setProperty("afterParseAction", "hide");
				log.info("Source files will be hidden when processed.");
			}
		}
	}
	

	/**
	 * Enables use of parser debugging
	 */
	public void setDebugger(final ParserDebugger pd) {
		this.debugger = pd;
	}

	/**
	 * Parser implementation uses this method to get session logging information from parent. Parser implementation
	 * calls this method for each file.
	 */
	public void preParse(final SourceFile sf) throws Exception {

		sf.setBatchID(batchID++);
		synchronized(sizelock) {
			long fileSize = 0;
			fileSize =	sf.fileSize(); 
			totalSize += fileSize;
			log.finest("file info=" + sf.getName()+ "fileSize=" + fileSize + "totalSize=" + totalSize);
			}

		sf.setParsingstarttime(System.currentTimeMillis());
		sf.setParsingStatus("STARTED");

		if (debugger != null) {
			debugger.started();
		}

		log.finest("Executing parse. batchID " + sf.getBatchID());

		if (sf.getErrorFlag()) {
			throw new Exception(sf.getErrorMsg());
		}

	}

	/**
	 * Parser implementation calls this method after successfull parsing for each file.
	 */
	public void postParse(SourceFile sourceFile) throws Exception {

		int count = decrementZipEntries(sourceFile);
		log.finest("Parsed meastypes: " + sourceFile.getMeastypeList().toString());
		synchronized (measSet) {
			measSet.addAll(sourceFile.getMeastypeList());
		}

		log.finer("Parsed file: " + sourceFile.getName());
		flog.info(sourceFile.getName() + " parsed.");

		final String aftParseAction = conf.getProperty("afterParseAction", "");

		final String parsername = conf.getProperty("parserType");
		if (aftParseAction.equals("delete") || aftParseAction.equals("hide")) {
			// Start Code changes for TR HP47608
			if (parsername.equalsIgnoreCase("alarm")) {
				sourceFile.delete();
			} else {
				// sourceFile.delete();
				if (count == 0) {
					if (sourceFile instanceof ZipEntrySource) {
						SourceFile tempSourceFile = zipSourceFiles.get(((ZipEntrySource) sourceFile).getParentName());
						if (tempSourceFile != null) {
							sourceFile = tempSourceFile;
							sourceFileList.add(sourceFile);
						}
					}
				} else if (count == -1) {
					sourceFileList.add(sourceFile);
				}
				
			}
		} else if (aftParseAction.equals("no")) {
			if (count == 0) {
				if (sourceFile instanceof ZipEntrySource) {
					SourceFile tempSourceFile = zipSourceFiles.get(((ZipEntrySource)sourceFile).getParentName());
					if (tempSourceFile != null) {
						sourceFile = tempSourceFile;
					}
				}
			}
			log.info("Input file " + sourceFile.getName() + " left into input directory");
		} else {
			if (count == 0) {
				if (sourceFile instanceof ZipEntrySource) {
					SourceFile tempSourceFile = zipSourceFiles.get(((ZipEntrySource)sourceFile).getParentName());
					if (tempSourceFile != null) {
						sourceFile = tempSourceFile;
					}
				}
				sourceFile.move(archiveDir);
			} else if (count == -1) {
				sourceFile.move(archiveDir);
			}
			
		}
		sourceFile.setParsingStatus("OK");
	}

	/**
	 * Parser implementation calls this method after unsuccessfull parsing for each file.
	 */
	public void errorParse(final Exception e, SourceFile sf) throws Exception {

		log.log(Level.WARNING, "Error parsing file " + sf.getName(), e);
		int count = decrementZipEntries(sf);

		final String failedParseAction = conf.getProperty("failedAction", "move");

		if (failedParseAction.equalsIgnoreCase("move")) {
			sf.move(failedDir);
		} else if (!failedParseAction.equalsIgnoreCase("no")) {
			//Treat it the same as file that was parsed successfully (usually it's delete but possibly hide during rolling upgrade)
			final String aftParseAction = conf.getProperty("afterParseAction", "");
			if (aftParseAction.equals("delete")) {
				if (count == 0) {
					if (sf instanceof ZipEntrySource) {
						SourceFile tempSourceFile = zipSourceFiles.get(((ZipEntrySource)sf).getParentName());
						if (tempSourceFile != null) {
							sf = tempSourceFile;
						}
					}
					sf.delete();
				} else if (count == -1) {
					sf.delete();
				}
				
			} else if (aftParseAction.equals("hide")) {
				if (count == 0) {
					if (sf instanceof ZipEntrySource) {
						SourceFile tempSourceFile = zipSourceFiles.get(((ZipEntrySource)sf).getParentName());
						if (tempSourceFile != null) {
							sf = tempSourceFile;
						}
					}
					sf.hide(rollingHideDir);
				} else if (count == -1) {
					sf.hide(rollingHideDir);
				}
			}
		} else {
			log.info("File left in indir, as instructed by interface");
		}

		flog.info(sf.getName() + " parsing failed.");

		sf.setParsingStatus("ERROR");
		sf.setErrorMessage(e.getMessage());

	}
	
	private int decrementZipEntries(SourceFile sf) {
		log.log(Level.FINEST, "decrementZipEntries: sf.getName() :"+sf.getName());
		if (sf instanceof ZipEntrySource ) {
			log.log(Level.FINEST, "decrementZipEntries: sf.getParentName() :"+((ZipEntrySource) sf).getParentName());
			String fileName = ((ZipEntrySource) sf).getParentName();
			int count = zipCountMap.get(fileName);
			if (count == 0) {
				return count;
			} else {
				count--;
				zipCountMap.put(fileName, count);
			}
			log.log(Level.FINEST, "decrementZipEntries: count :"+count);
			return count;
		}
		return -1;
	}

	/**
	 * Parser implementation calls this method after end of parsing for each file.
	 */
	public void finallyParse(final SourceFile sf) throws Exception {

		sf.setParsingendtime(System.currentTimeMillis());

		final Map lentry = sf.getSessionLog();
		synchronized (parserSessionLogs) {
			parserSessionLogs.add(lentry);
		}
		final int bulkLimit = SessionHandler.getBulkLimit();
		if (parserSessionLogs.size() >= bulkLimit) {
			log.finest("The number of session is currently > " + bulkLimit + ". Performing write to SessionLog.");
			synchronized (parserSessionLogs) {
				SessionHandler.bulkLog(Main.SESSIONTYPE, parserSessionLogs);
				log.finest("Finished Writing to the SessionLog.");
				// Now clear the collection...
				parserSessionLogs.clear();
			}
		}

		final int rows = sf.getRowCount();
		allRows += rows;
		log.finer("Parsed file " + sf.getName() + " resulting " + rows + " rows.");

		// Takes care that no open files remain
		sf.close();

		log.finer("Closed measurement files of parsed source file " + sf.getName());

	}

	public boolean isBrokenLink(final File file, final int cType) {

		if (cType == 0) {
			if (file.exists()) {
				return false;
			} else {
				return true;
			}
		} else if (cType == 1) {
			if (file.exists()) {
				return false;
			} else {
				return true;
			}
		} else if (cType == 2) {
			if (file.isFile()) {
				return false;
			} else {
				return true;
			}
		} else if (cType == 3) {
			if (file.canRead()) {
				return false;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}
	
	/**
	 * 
	 */
	public void trackBrokenLinkFiles(final File brokenLink,final File targetFailedDir) {
	      SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
	      File trackFile=new File(targetFailedDir,"trackfile_"+sdf.format(new Date())+".txt");
	      try {
	    	  boolean fileExist=trackFile.createNewFile();
	    	  if(fileExist)
	          {
	        	  log.finest("Track file "+trackFile+" created.");
	          }
	    	  else
	    	  {
	    		  log.finest("Track file "+trackFile+" already exist");
	    	  }
	      }
	      catch(Exception e)
	      {
	    	  log.warning("Track file "+trackFile+" creation failed. "+e);
	    	  return;
	      }
	      
	      Path path=Paths.get(trackFile.getPath());
	      boolean toWrite=false;
	      try(Stream<String> stream=Files.lines(path);)
	      {
	    	  if(!stream.filter(brokenFile->brokenFile.contains(brokenLink.getName())).findAny().isPresent())
	          {
	    		  toWrite=true;	        	  
	          }
	    	  else
	    	  {
	    		  log.finest("File " + brokenLink.getName() + " is already tracked in the "+trackFile);
	    	  }
	    	 
	    	  
	      }
	      catch(Exception e)
	      {
	    	  log.warning("Error while writing  to the track file " + trackFile + " " +e);
	      }
	      if(toWrite)
	      {
	    	  try(FileOutputStream output =new FileOutputStream(trackFile, true);
		    		  PrintWriter writer=new PrintWriter(output);)
		      {
	    		  Path brokenLinkPath=Paths.get(brokenLink.getPath());
	    		  Path originalFile=Files.readSymbolicLink(brokenLinkPath);
	    		  String fileDate=sdf.format(new Date());
	        	  writer.println(fileDate+" "+originalFile+" "+brokenLinkPath);
	        	  log.finest("File " + brokenLink.getName() + " successfully tracked in the "+trackFile);
	    		  brokenLink.delete();
				  flog.info(brokenLink.getName() + " deleted as broken link."); 
		      }
		      catch(Exception e)
		      {
		    	  log.warning("Error while writing  to the track file " + trackFile + " " +e);
		      }
	      }
	      		
	}

	/**
	 * 
	 */
	public SourceFile nextSourceFile() throws Exception {

		while (true) {
			FileInformation fi = null;

			synchronized (this) {
				if ((fileList.size() > 0) ) {
					fi = fileList.remove(0);
					fileCount++;
				} else {
					return null;
				}
			}

			SourceFile sf = null;
			if (fi.isZipEntry) {
				ZipEntrySource zes = new ZipEntrySource(fi.file, fi.zipEntry, fi.memoryConsumptionMB, conf, rf,
						psession, debugger, log);
				sf = zes;
				String parentFileName = zes.getParentName();
				SourceFile parentSourceFile = zipSourceFiles.get(parentFileName);
				if (parentSourceFile == null) {
					parentSourceFile = new SourceFile(fi.file, fi.memoryConsumptionMB, conf, rf, psession, debugger,
							conf.getProperty("useZip", "false"), log);
					zipSourceFiles.put(parentFileName, parentSourceFile);
				}
			} else {
				if (!isBrokenLink(fi.file, checkType)) {
					sf = new SourceFile(fi.file, fi.memoryConsumptionMB, conf, rf,psession, debugger,
							conf.getProperty("useZip", "false"), log);
				} else {
					log.finest("File " + fi.file.getName() + " Does not exists.");

					// File is probably a broken link -> delete

					try {
						trackBrokenLinkFiles(fi.file,failedDir);
						//fi.file.delete();
						//flog.info(fi.file.getName() + " deleted as broken link.");
					} catch (final Exception e) {
						log.fine("Could not delete suspected broken link" + fi.file.getName());
					}

					continue;
				}
			}
		
			final long start_pf = System.currentTimeMillis();

			try {

				if ((checker != null)
						&& !fi.isZipEntry && checker.isProcessed(sf.getName(),
								checker.getProcessedDir(conf.getProperty("ProcessedFiles.processedDir"))
										+ File.separator + conf.getProperty("interfaceName"))) {
					// if (checker != null && checker.isProcessed(sf.getName(),
					// conf.getProperty("interfaceName"))) {
					log.fine("File " + sf.getName() + " already parsed.");

					final String doubleParseAction = conf.getProperty("doubleCheckAction", "move");

					if ("move".equals(doubleParseAction)) {
						sf.trackDuplicatePMFiles(doubleDir);
					} else if ("delete".equals(doubleParseAction)) {
						sf.delete();
					}

					flog.info(sf.getName() + " ignored as duplicate file.");

					continue;
				}

			} catch (final Exception e) {
				sf.setErrorFlag(true);
				sf.setErrorMsg(e.getMessage());
				return sf;
			}
			performanceLog.finer("duplicate check isProsessed took " + (System.currentTimeMillis() - start_pf) + " ms");

			if (sf.fileSize() == 0) {
				log.fine("File " + sf.getName() + " Empty file");
				sf.move(failedDir);
				flog.info(sf.getName() + " Move to failed.");

				continue;
			}

			log.finer("Delegating file " + sf.getName());
			return sf;

		}

	}

	// CR 102310918-FCP 103 8147 Filename Filter
	// fileNameFilter gets a value from the property set by TP or in
	// static.properties
	private String determineFileFilterNameproperty() {

		// parser specific filenamefilter property defined on the fly in
		// static.properties file
		final String parserSpecificFileNameFilter = StaticProperties.getProperty(parserType + "."
				+ "inputfile.filenamefilter", null);
		// Default filenamefilter property defined on the fly in static.properties
		// file
		final String commomnFileNameFilter = StaticProperties.getProperty("inputfile.filenamefilter", null);
		;
		// Property is defined at the time of Tech Pack creation
		final String tpDefinedfileNameFilter = conf.getProperty("inputfile.filenamefilter", null);
		// Default property reads all files
		final String defaultFileNameFilter = "";

		String fileFilterValidationStatus = "";

		if (parserSpecificFileNameFilter != null) {
			this.fileNameFilter = parserSpecificFileNameFilter;

			log.finest("Assigned parser " + parserType + " specific filenamefilter property:" + " "
					+ parserSpecificFileNameFilter + ", defined in static.properties file");

			fileFilterValidationStatus = "Assigned parser " + parserType + " specific filenamefilter property:" + " "
					+ parserSpecificFileNameFilter + ", defined in static.properties file";
		} else if (commomnFileNameFilter != null) {
			this.fileNameFilter = commomnFileNameFilter;

			log.finest("Assigned default filenamefilter property: " + parserSpecificFileNameFilter
					+ ", defined in static.properties file");

			fileFilterValidationStatus = "Assigned default filenamefilter property:" + parserSpecificFileNameFilter
					+ ", defined in static.properties file";
		} else if ((parserSpecificFileNameFilter == null) && (commomnFileNameFilter == null)
				&& (tpDefinedfileNameFilter != null)) {
			this.fileNameFilter = tpDefinedfileNameFilter;

			log.finest("Assigned TP defined filenamefilter property: " + tpDefinedfileNameFilter
					+ ", defined in Tech Pack");

			fileFilterValidationStatus = "Assigned TP defined filenamefilter property:" + tpDefinedfileNameFilter
					+ ", defined in tech pack";
		} else if ((parserSpecificFileNameFilter == null) && (commomnFileNameFilter == null)
				&& (tpDefinedfileNameFilter == null)) {
			this.fileNameFilter = defaultFileNameFilter;
			log.finest("Assigned default filenamefilter property: " + parserSpecificFileNameFilter + ",defined in "
					+ Main.class.getName());

			fileFilterValidationStatus = "Assigned TP defined filenamefilter property:" + parserSpecificFileNameFilter
					+ ",defined in " + Main.class.getName();
		}
		return fileFilterValidationStatus;
	}

	/**private synchronized String addWorkersToQueue(final Map<String, Parser> workers) {

		String thisWorker = "";
		final Share sh = Share.instance();
		final PriorityQueue pq = (PriorityQueue) sh.get("priorityQueueObject");
		final Iterator<String> iter = workers.keySet().iterator();
		while (iter.hasNext()) {
			if (thisWorker.length() > 0) {
				final String key = iter.next();
				final Parser p = workers.get(key);
				final EngineThread et = new EngineThread(set_name + "_" + key, "Adapter", new Long(100), p, log);
				et.setName("parserWorker" + key);
				pq.addSet(et);
			} else {
				thisWorker = iter.next();
				final ExecutionMemoryConsumption emc = ExecutionMemoryConsumption.instance();
				emc.add(workers.get(thisWorker));
			}
		}

		return thisWorker;
	}*/

	/**
	 * 
	 * return true if all workerlists workers statuses are in status, if one ore more statusses are not in status return
	 * false
	 * 
	 * @param workerList
	 * @param status
	 * @return
	 */
	private boolean allWorkerStatusesAre(final Map<String, Parser> workers, final int status) {
		this.log.log(Level.FINEST, "Checking if worker statuses in " + status);
		final Iterator<String> iter = workers.keySet().iterator();
		while (iter.hasNext()) {
			final String key = iter.next();
			if ((workers.get(key)).status() != status) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 
	 * return true if all workerlists workers statuses are not in status, if one ore more statusses are in status return
	 * false
	 * 
	 * @param workerList
	 * @param status
	 * @return
	 */
	private boolean noWorkerStatusIs(final Map<String, Parser> workers, final int status) {
		this.log.log(Level.FINEST, "Checking if worker statuses not in " + status);
		final Iterator<String> iter = workers.keySet().iterator();
		while (iter.hasNext()) {
			final String key = iter.next();
			if ((workers.get(key)).status() == status) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 
	 * String of workers and their statuses
	 * 
	 * @param workerList
	 * @param status
	 * @return
	 */
	private String workerStatusString(final Map<String, Parser> workers) {
		String result = " ";
		final Iterator<String> iter = workers.keySet().iterator();
		while (iter.hasNext()) {
			final String key = iter.next();
			result += (workers.get(key)).status() + " ";
		}
		return result;
	}

	/**
	 * Executes the parsing action. Blocks until parsing is finished.
	 * 
	 * @return Map containing: Number of parseable files on the in-directory after the parse has finished. List of
	 *         measurement types that where parsed.
	 * @throws Exception
	 *             in case of fatal error. Failure of
	 */
	public Map<String, Set> parse() throws Exception {

		parse_start = System.currentTimeMillis();

		final String interfaceName = conf.getProperty("interfaceName");

		checkType = Integer.parseInt(conf.getProperty("brokenLinkCheck", "0"));
		
		enginerobustflag=conf.getProperty("enginerobustflag", "false");
		if(enginerobustflag.equalsIgnoreCase("true")){
			log.info("using engine improvement. ");
		}

		dfCache = DataFormatCache.getCache();

		if (!dfCache.isAnInterface(interfaceName)) {
			log.info("Interface " + interfaceName + " not found or not active");
			return new HashMap<String, Set>();
		}

		log.finest("Start parsing...");

		// returns true if ok, false otherwise.
		if (!checkDirectories()) { // Exception if directories fail
			return new HashMap();
		}

		Map<String, Parser> workers = null;

		// handle the possible pending temporary files first
		log.finest("Starting to handle pending temporary files...");
		// Code changes for TR HP47608
		if (conf.getProperty("parserType").equalsIgnoreCase("alarm")) {
			handlePendingTempFiles(conf.getProperty("outDir"), techPack,
					Integer.parseInt(conf.getProperty("Parser.tempFileHandlingCase", "3")));
		} else {
			handlePendingTempFiles(conf.getProperty("outDir"), techPack,
					Integer.parseInt(conf.getProperty("Parser.tempFileHandlingCase", "1")));
		}
		try {

			fileList = createFileList();

			if (fileList.size() <= 0) { // No files to parse -> return
				log.info("No valid files found from IN directories ");
				// unclock all locked dirs first
				this.unlockDirs();
				return new HashMap();
			}

			log.info("File list created. " + fileList.size() + " files to be parsed.");

			// sort file list by size in descending order if useMemoryRestriction is
			// true
			if (useMemoryRestriction) {
				log.info("Starting to sort file list into descending order by file size.");
				Collections.sort(fileList, new FileInformationComparator());
				log.info("File list sorted.");
			}

			final long sessionID = SessionHandler.getSessionID(SESSIONTYPE);
			log.fine("This session has ID: " + sessionID);

			psession = new ParseSession(sessionID, conf);

			// Creates an instance of parser implementation
			workers = createParserWorkers();
			if(enginerobustflag.equalsIgnoreCase("true")){
			long startTime = System.currentTimeMillis();
			final int avgfilesize=(fileListSize()*1024*1024)/(fileList.size());
			if((workers.size()<=2) && (avgfilesize > 10*1024*1024)){
				List<FileInformation> newfileList = new ArrayList<FileInformation>();
				this.newFileSize = 0L;
				long size =0; 
				if(fileList.size() > 2){
					int FileListsize= (int)(fileList.size()/2);
					for (int i = 0; i < FileListsize; i++) {
						newfileList.add(fileList.get(i));
						size = size+fileList.get(i).fileSizeB;
					}
					long endTime = System.currentTimeMillis();
					int newSize = fileList.size()-newfileList.size();
					this.log.info("Rejecting "+newSize+" files due to Memory Constraint "+(endTime-startTime)+"ms");
					this.fileList.clear();
					this.fileList = newfileList;
					this.newFileCount=this.fileList.size();
					this.newFileSize=size/1024;
					log.info("Number of files:"+newFileCount+" File Size is:"+newFileSize);
					
				}
			  }
			}
			final String regExp = conf.getProperty("ProcessedFiles.fileNameFormat", "");
			if (!conf.getProperty("dublicateCheck", "true").equals("false") && (regExp != null)
					&& (regExp.length() > 0)) {
				checker = new ProcessedFiles(conf);
			}

			log.log(Level.FINE, "workerList created... workerList size is " + workers.size());
			//final String thisWorker = addWorkersToQueue(workers);
			log.log(Level.FINE, "Added workers to queue...");

			batchID = 0;
			measSet = new HashSet<String>();
			allRows = 0;

			// run one worker here..
			/**if ((thisWorker.length() > 0) && (workers.size() > 0)) {
				this.log.log(Level.FINE, "Running one worker in here.");
				//Fix for EQEV-48284 
				//Reason: run method of one worker is being called twice. Hence commenting.
				//(workers.get(thisWorker)).run();
				this.internalWorker = workers.get(thisWorker);
				this.internalWorker.run();
				final ExecutionMemoryConsumption emc = ExecutionMemoryConsumption.instance();
				emc.remove(workers.get(thisWorker));
			}*/

			// waiting worker parsers to end or parsed files to end.
			while (!allWorkerStatusesAre(workers, 3)) {
				// update memory consumption of the queued workers
				updateQueuedWorkersMemoryConsumption(workers);

				// wait untill all worker parsers are in status 3 (finished)
				Thread.sleep(1000);
				this.log.log(Level.FINEST, "Files left: " + fileList.size() + " worker statuses:"
						+ workerStatusString(workers));
				// if all files have been delgated to workers and no worker is no longer
				// running (status=2) no need to wait for
				if ((fileList.size() <= 0) && noWorkerStatusIs(workers, 2)) {
					// break if no files left and no worker in 2 parsing
					break;
				}
			}

			synchronized (parserSessionLogs) {
				if (!parserSessionLogs.isEmpty()) {
					log.finest("Writing remaining " + Main.SESSIONTYPE + " logs ...");
					SessionHandler.bulkLog(Main.SESSIONTYPE, parserSessionLogs);
					parserSessionLogs.clear();
				}
			}

			this.unlockDirs();
			// Fix for the TR:HM90749
			if (allRows == 0) {
				log.warning("Parsing results in " + allRows
						+ "row, because the input file we are parsing doesn't results "
						+ "in any value, Please check the input files which is parsed");
			}
			if(newFileCount<1){
					log.fine("Parse has finished. " + fileCount + " files parsed resulting " + allRows + " rows.");
				}else{
					log.fine("Parse has finished. " + this.newFileCount + " files parsed resulting " + allRows + " rows.");
				}
			

			final Map<String, Set> result = new HashMap<String, Set>();
			synchronized (result) {
				result.put("parsedMeastypes", measSet);
			}

			final long time = (System.currentTimeMillis() - parse_start) / 1000;
			final long size = totalSize / 1024;
			if(newFileCount<1){
				performanceLog.info("Parsed: " + fileCount + " files, " + size + "Kb, " + allRows + " rows in " + time
					+ " secs");
			}else{
				performanceLog.info("Parsed: " + this.newFileCount + " files, " + this.newFileSize + "Kb, " + allRows + " rows in " + time
						+ " secs");
			}
			if (time > 1) {
				if(newFileCount<1){
					performanceLog.info("   " + (this.newFileSize / time) + " Kb/s " + (allRows / time) + " rows/s");
				}else{
					performanceLog.info("   " + (size / time) + " Kb/s " + (allRows / time) + " rows/s");
				}
			}
			
			if (debugger != null) {
				debugger.finished();
			}

			moveFilesToLoader();

			if (checker != null) {
				checker.writeProcessedToFile();
			}

			return result;

		} catch (final Exception ex) {

			// Exception catched at top level. No good.
			log.log(Level.WARNING, "Parser failed to exception", ex);

			if (debugger != null) {
				debugger.failed(ex);
			}

			throw ex;
		} finally {
			this.unlockDirs();
		}

	}

	public void updateQueuedWorkersMemoryConsumption(final Map<String, Parser> workers) {
		if (null == workers) {
			return;
		}

		if (workers.size() > 0) {
			final Iterator<String> it = workers.keySet().iterator();
			while (it.hasNext()) {
				final String key = it.next();
				final Object worker = workers.get(key);
				if (worker instanceof MemoryRestrictedParser) {
					final MemoryRestrictedParser mrp = ((MemoryRestrictedParser) worker);
					// if worker in queue update the memory consumption
					if (1 == mrp.status()) {
						mrp.setMemoryConsumptionMB(getNextSFMemConsumptionMB());
					}
				}
			}
		}

	}

	/**
	 */
	public static MeasurementFile createMeasurementFile(final SourceFile sourceFile, final String tagID,
			final String techPack, final String set_type, final String set_name, final Logger log) throws Exception {

		return createMeasurementFile(sourceFile, tagID, techPack, set_type, set_name, "", log);

	}

	/**
	 * Creates a MeasurementFile. MeasurementFile references are are stored. All open measurementFiles are closed when
	 * parse-method exits.
	 * 
	 * @param sourceFile
	 *            SourceFile that parser is parsing
	 * @param tagID
	 *            tagID the parser instance is using.
	 * @return MeasurementFile Created measurmentFile
	 * @throws Exception
	 *             if initialization fails
	 */
	public static MeasurementFile createMeasurementFile(final SourceFile sourceFile, final String tagID,
			final String techPack, final String set_type, final String set_name, final String workerName,
			final Logger log) throws Exception {
		final MeasurementFile mf = new MeasurementFileImpl(sourceFile, tagID, techPack, set_type, set_name, workerName,
				log);
		sourceFile.addMeasurementFile(mf);
		return mf;
	}

	public static void removeMeasurementFile(final SourceFile sFile, final MeasurementFile mFile) throws Exception {
		if (!mFile.hasData()) {
			sFile.removeMeasurementFile(mFile);
			mFile.close();
		}
	}

	private int fileListSize() {
		long size = 0;

		final Iterator<FileInformation> iterator = fileList.iterator();

		if (useMemoryRestriction) {
			while (iterator.hasNext()) {
				final FileInformation f = iterator.next();
				size += f.fileSizeB;
			}
		} else {
			while (iterator.hasNext()) {
				final FileInformation f = iterator.next();
				size += f.file.length();
			}
		}

		final int mbsize = (int) (size / 1024 / 1024);

		log.fine("File list size is " + mbsize + " MB");

		return mbsize;
	}
	

	public int getNextSFMemConsumptionMB() {
		if (fileList.size() > 0) {
			final FileInformation fi = fileList.get(0);
			return fi.memoryConsumptionMB;
		}
		return 0;
	}

	/**
	 * Work around for a null being returned by the listFiles() in the case of an IO error.
	 * 
	 * @param inDir
	 * @return File list, or null if the read failed after 3 attempts
	 * @throws Exception
	 *             If the inDir is a File and not a directory.
	 */
	protected File[] fileList(final File inDir) {
		if (inDir.isFile()) {
			log.warning("$inDir " + inDir.getAbsolutePath() + " is not a directory");
			return new File[0];
		}
		File[] list;
		int retry = 0;
		do {
			list = inDir.listFiles();
			retry++;
		} while ((list == null) && (retry < 3));
		if (list == null) {
			log.finest("$inDir " + inDir.getAbsolutePath() + " could not be read, consecutive nulls from IO");
			return new File[0];
		} else {
			return list;
		}
	}

	/**
	 * Creates a list of handled files.
	 * 
	 * @return a List of SourceFiles.
	 */
	private List<FileInformation> createFileList() {

		final List<FileInformation> resultList = new ArrayList<FileInformation>();

		final long start = System.currentTimeMillis();

		final boolean timestampCheck = (StaticProperties.getProperty("Adapter.TimestampCheck", "true")
				.equalsIgnoreCase("true")) ? true : false;

		File inDir = null;

		String directory = resolveDirVariable(conf.getProperty("inDir", null));

		if (directory == null) {

			directory = resolveDirVariable(conf.getProperty("baseDir"));

			if (!directory.endsWith(File.separator)) {
				directory += File.separator;
			}

			inDir = new File(directory + "in");

		} else {

			inDir = new File(directory);

		}

		// inDir is ready

		int timeDiff = 0;
		try {
			timeDiff = Integer.parseInt(conf.getProperty("minFileAge"));
		} catch (final NumberFormatException e) {
			log.config("minFileAge not defined/malformed -> Parsing all");
		}

		int fileCountLimit = MAXFILECOUNT;

		try {
			fileCountLimit = Integer.parseInt(conf.getProperty("maxFilesPerRun"));
		} catch (final Exception e) {
			log.config("maxFilesPerRun not defined/malformed");
		}
		
		if(checkMultiBlade().equalsIgnoreCase("multiblade")){
			fileCountLimit = ((int) (fileCountLimit * 0.5) + fileCountLimit);
			
		}
		
		if ((fileCountLimit <= 0) || (fileCountLimit > MAXFILECOUNT)) {
			log.config("maxFilesPerRun can be [1," + MAXFILECOUNT + "]");
			fileCountLimit = MAXFILECOUNT;
		}

		
		log.finest("Parsing " + fileCountLimit + " files per run. Files that are at least " + timeDiff + " minutes old");

		final String tMethod = conf.getProperty("thresholdMethod", "less");

		// if thresholdMethod is neither less or more we nag and exit.
		if (!tMethod.equalsIgnoreCase("less") && !tMethod.equalsIgnoreCase("more")) {
			log.warning("Unknown dir threshold method: " + tMethod + " no parsable files found.");
			return resultList;
		}

		long dirThreshold = 24L;
		try {
			dirThreshold = Integer.parseInt(conf.getProperty("dirThreshold", "24"));
		} catch (final NumberFormatException nfe) {
			log.config("dirThreshold parameter is invalid. Defaulting to 24");
		}

		if (timestampCheck) {
			if (tMethod.equalsIgnoreCase("less")) {
				log.fine("Parsing subdirectories with age less than " + dirThreshold + " hours.");
			} else {
				log.fine("Parsing subdirectories with age more than " + dirThreshold + " hours.");
			}
		} else {
			log.fine("Skipping timestamp check, parsing all subdirectories");
		}

		dirThreshold *= 360000; // Making it hours

		long startlist = System.currentTimeMillis();

		performanceLog.finest("File listing parameters solved in " + (startlist - start) + "ms");

		// Listing parameters are handled.
		final File[] f = fileList(inDir);

		// CR 102310918-FCP 103 8147 Filename Filter
		// New constructor with new parameter to support filtering file
		final ParserFileFilter pff = new ParserFileFilter(fileCountLimit, checkType, fileNameFilter);

		this.log.log(Level.FINE, "Parser's IN directory is " + inDir + " and it contains " + f.length
				+ " file entries.");

		for (final File element : f) {

			startlist = System.currentTimeMillis();

			this.log.log(Level.FINER, "Iterating at " + element.getName());
			
			if (element.getName().startsWith(".nfs")){
				this.log.log(Level.WARNING, "Found a hidden file and ignored by parser file collection. FileName : " +element.getName());
				continue;
			}

			final Long thresholdTime = new Long(System.currentTimeMillis() - dirThreshold);

			if (element.isDirectory() && !element.getName().equalsIgnoreCase(rollingHideDir)) {

				// The file entry is a directory in IN directory.
				// check for equality
				// check lock file
				if (isDirLocked(element)) {
					// lock file found
					log.fine("Input directory (" + element.getAbsolutePath() + ") locked by other adapter");
				} else {

					if (timestampCheck) { // If TimestampCheck property is set false in
						// static.properties this timestamp checking is skipped
						final long lastModTime = element.lastModified();

						if (tMethod.equalsIgnoreCase("less")) {
							// Directory or file modified less than dirThreshold ago.
							if (lastModTime > thresholdTime.longValue()) {
								// ok = true;
							} else {
								this.log.log(Level.FINEST, "File entry last modified time " + element.lastModified()
										+ " is smaller than threshold time " + thresholdTime.longValue());
								continue;
							}

						} else if (tMethod.equalsIgnoreCase("more")) {
							// Directory or file modified more than dirThreshold ago.
							if (lastModTime <= thresholdTime.longValue()) {
								// ok = true;
							} else {
								this.log.log(Level.FINEST, "File entry last modified time " + element.lastModified()
										+ " is equal or larger than threshold time " + thresholdTime.longValue());
								continue;
							}

						}
					}

					final int oldFilesAccepted = pff.filesAccepted;
					final int oldFilesRejected = pff.filesRejected;

					final File[] ret = element.listFiles(pff);
					
					for (final File element2 : ret) {
						// resultList.add(ret[fi]);
						
						if (element2.getName().startsWith(".nfs")){
							this.log.log(Level.WARNING, "Found a hidden file and ignored by parser file collection. FileName : " +element2.getName());
							continue;
						}
						FileInformation fileInformation = null;
						
						if (element2.getName().endsWith(".zip")){
							if (!handleZipFile(element2, resultList)) {
								continue;
							}
						} else {
							if (useMemoryRestriction) {
								fileSize = element2.length();
								fileInformation = new FileInformation(element2, fileSize, ((int) fileSize)
										* memoryUsageFactor);
							} else {
								fileInformation = new FileInformation(element2, 0, 0);
							}

							resultList.add(fileInformation);
						}
						
						if(enginerobustflag.equalsIgnoreCase("true")){
							totalfilesize+=fileSize;
							log.finest("total size of files listed   " + totalfilesize + " bytes.");
							int nooffiles=resultList.size();
							int avgfilesize=(int) (totalfilesize/nooffiles);
							if((avgfilesize * 29 > Runtime.getRuntime().freeMemory() * .2) || (avgfilesize > 10 * 1024 *1024)){
								if ((totalfilesize * 3 > Runtime.getRuntime().totalMemory()) || (totalfilesize * 10 > Runtime.getRuntime().maxMemory()) || (totalfilesize  > Runtime.getRuntime().freeMemory())){	
									log.info("overriding maxfileperrun  " + nooffiles + " average file size " + avgfilesize);
									int overhead=pff.filesAccepted-oldFilesAccepted-nooffiles;
									pff.filesAccepted=oldFilesAccepted+nooffiles;
									pff.filesRejected=pff.filesRejected + overhead;
									pff.setFileCountLimit(nooffiles);
									break;
								}
							}
						}	
					}
					final int filesAccepted = pff.filesAccepted - oldFilesAccepted;
					final int filesRejected = pff.filesRejected - oldFilesRejected;

					log.finest("Fetched " + filesAccepted + " files (" + filesRejected + " files are not considered due to maximum files per run are exceeded) from "
							+ element.getAbsolutePath() + " in " + (System.currentTimeMillis() - startlist) + " ms");

					// found files , lock.
					if (ret.length > 0) {
						lockDir(element);
					}

				}

			} else {

				if (pff.accept(element) && !element.isDirectory()) {
					FileInformation fileInformation = null;
					if (element.getName().endsWith(".zip")){
						if (!handleZipFile(element, resultList)) {
							continue;
						}
					} else{
						if (useMemoryRestriction) {
							fileSize = element.length();
							fileInformation = new FileInformation(element, fileSize, ((int) fileSize) * memoryUsageFactor);
						} else {
							fileInformation = new FileInformation(element, 0, 0);
						}
						resultList.add(fileInformation);
					}
							
					if(enginerobustflag.equalsIgnoreCase("true")){
						totalfilesize+=fileSize;
						log.finest("total size of files listed   " + totalfilesize + " bytes.");
						int nooffiles=resultList.size();
						int avgfilesize=(int) (totalfilesize/nooffiles);
						if((avgfilesize * 29 > Runtime.getRuntime().freeMemory() * .2) || (avgfilesize > 10 * 1024 *1024)){
							if ((totalfilesize * 3 > Runtime.getRuntime().totalMemory()) || (totalfilesize * 10 > Runtime.getRuntime().maxMemory()) || (totalfilesize > Runtime.getRuntime().freeMemory())){	
								log.info("overriding maxfileperrun  " + nooffiles + " average file size " + avgfilesize);
								pff.setFileCountLimit(nooffiles);
							}
						}
					}
				}
			}
			
			// if we have collected all the files we can, exit.
			if (pff.filesAccepted >= fileCountLimit) {
				log.finest("Filelist size is " + pff.filesAccepted + " exiting iteration.");
				break;
			}

		} // foreach file in inDir

		performanceLog.info("Filelist created " + pff.filesAccepted + " files (" + pff.filesRejected
				+ " files are not considered due to maximum files per run are exceeded). " + (System.currentTimeMillis() - start) + " ms");

		return resultList;
	}
	
	private boolean handleZipFile(File file, List<FileInformation> resultList) {
		FileInformation fileInformation = null;
		if (file.getName().endsWith(".zip")){
			ZipFile zipFile = null;
			try {
				if (isBrokenLink(file,checkType)) {
					// File is probably a broken link -> delete
					try {
						file.delete();
						flog.info(file.getName() + " deleted as broken link.");
					} catch (final Exception e) {
						log.fine("Could not delete suspected broken link" + file.getName());
					}
					return false;
				}
				zipFile = new ZipFile(file);
				Enumeration<? extends ZipEntry> ez = zipFile.entries();
				if (zipCountMap == null) {
					zipCountMap = new ConcurrentHashMap<>();
				}
				int count =0;
				while(ez.hasMoreElements()) {
					ZipEntry zipEntry = ez.nextElement();
					count++;
					if (useMemoryRestriction) {
						fileSize = zipEntry.getSize();
						fileInformation = new FileInformation(file, zipEntry, fileSize, ((int) fileSize)
								* memoryUsageFactor);
					} else {
						fileInformation = new FileInformation(file, zipEntry, 0, 0);
					}
					resultList.add(fileInformation);
				}
				if (count != 0) {
					zipCountMap.put(file.getName(), count);
					log.log(Level.FINEST, "zipCountMap :"+zipCountMap);
				}
				return true;
			} catch (Exception e) {
				log.log(Level.WARNING,"Exception while parsing "+ zipFile.getName(),e);
			}
		} 
		return false;
	}

	/**
	 * 
	 * return true if dir is locked in share but not locked localy
	 * 
	 * 
	 * @param dir
	 * @return
	 */
	private boolean isDirLocked(final File dir) {

		if (localDirLockList.contains(dir)) {

			//final Share share = Share.instance();
			List list = new ArrayList<>();

			//final List list = (List) share.get("lockedDirectoryList");
			if (list == null) {
				return false;
			} else {
				return list.contains(dir);
			}

		} else {
			return false;
		}

	}

	/**
	 * 
	 * add dir to local and shares locked directory list.
	 * 
	 * 
	 * @param dir
	 */
	private void lockDir(final File dir) {
		// add lock file
		try {

			//final Share sh = Share.instance();
			/**if (!sh.contains("lockedDirectoryList")) {
				sh.add("lockedDirectoryList", new ArrayList());
			}*/
			//final List list = (List) sh.get("lockedDirectoryList");
			List list = new ArrayList<>();

			if (!localDirLockList.contains(dir)) {
				list.add(dir);
				localDirLockList.add(dir);
			}

			log.fine("Locking directory " + dir);

		} catch (final Exception e) {
			new Exception("Could not lock directory " + dir);
		}

	}

	/**
	 * remove local locked directory list from share locked directory list.
	 * 
	 * 
	 */
	private void unlockDirs() {
		// add lock file
		try {

			final Iterator<File> iter = localDirLockList.iterator();
			while (iter.hasNext()) {
				final File dirLock = iter.next();
				
				//final List list = (List) sh.get("lockedDirectoryList");
				List list = new ArrayList<>();
				if (list != null) {
					list.remove(dirLock);
				}
				log.fine("Unlocking directory " + dirLock.getAbsolutePath());
			}

			localDirLockList.clear();

		} catch (final Exception e) {
			new Exception("Error while unlocking directories ");
		}

	}

	/**
	 * Moves files to correct output directory and give them extension .txt or .binary
	 * 
	 * @return
	 */
	private void moveFilesToLoader() throws Exception {

		final String eventsEtlDataDir = System.getProperty(CommonUtils.EVENTS_ETLDATA_DIR,
				CommonUtils.EVENTS_ETLDATA_DIR_VAL);
		final String outDirFromProps = resolveDirVariable(conf.getProperty("outDir", conf.getProperty("baseDir")
				+ File.separator + "out"));

		// if outDirFromProps does already contain /eniq/data/etldata_/, then treat
		// as if a single directory server
		if (outDirFromProps.contains(eventsEtlDataDir)) {
			NoOfDir = 0;
		} else {
			NoOfDir = CommonUtils.getNumOfDirectories(log);
		}

		log.info("Number of directories read from niq.ini file is :" + NoOfDir);

		if (NoOfDir == 0) {
			log.info("This is a single directory server");
			NoOfFileSystem = 1;
		} else {
			log.info("This is a " + NoOfDir + " directory server");
			NoOfFileSystem = NoOfDir;
		}
		
		int totalNumFiles = 0;
		int measfileMoveCount = 0;

		/*
		 * Ex : If single file system NoOfFileSytem = 1 If multi file system NoOfFileSytem = 4
		 */

		for (int folderNumber = 0; folderNumber < NoOfFileSystem; folderNumber++) {

			if (NoOfDir != 0) {
				log.info("Files being read from FileSystem :" + preAppend + folderNumber);
			}

			String outDir = outDirFromProps;

			final String delimiter = System.getProperty(CommonUtils.ETLDATA_DIR, CommonUtils.ETLDATA_DIR_DEFAULT);

			if (NoOfDir != 0) { // If not a single FSA Server
				preAppend = folderNumber <= 9 ? "0" : "";
				final String[] temp = outDir.split(delimiter, 2);
				outDir = temp[0] + eventsEtlDataDir + File.separator + preAppend + folderNumber + temp[1];
			}

			if (!outDir.endsWith(File.separator)) {
				outDir += File.separator;
			}

			outDir += techPack;

			log.finest("outDir is :" + outDir);

			// for TR HO54893
			final FilenameFilter filter = new FilenameFilter() {

				@Override
				public boolean accept(final File dir, final String name) {
					return !name.endsWith(WORKING_COPY_SUFFIX);
				}
			};//

			final File[] f = new File(outDir).listFiles();

			//if (f == null) {
			//	log.warning("MoveFilesToLoader: outDir (" + outDir + ") does not exists. Ignoring move.");
			//	return;
			//}


			
			if (f !=null) {
				log.finest("OutDirectory "+outDir+" exists");
				log.finest("No of files found under directory " + outDir + "is :" + f.length);
				totalNumFiles = totalNumFiles + f.length;
				for (final File outputFile : f) {
					
					
					if (outputFile.getName().toLowerCase().startsWith(".nfs"))
					{
					this.log.log(Level.WARNING, "Found a hidden file in "+outDir +", ignoring File : " +outputFile.getName());
		            measfileMoveCount++;
			        continue;
					}

					
					
					log.log(Level.FINE, "Iterating at file " + outputFile.getAbsolutePath());

					if (outputFile.length() == 0) {
					log.warning("Empty measurement file found: " + outputFile.getAbsolutePath()
							+ ".\nIt will NOT be moved to loader. Deleting file now.");
					outputFile.delete();
					continue;
					}

				String destDir = resolveDirVariable(conf.getProperty("loaderDir", ""));

				// For Testing Interface on standalone
				if (conf.getProperty("interfaceTester", "false").equals("true")) {
					log.log(Level.INFO, "Testing Interface - move files to Loader skipped");
					break;
				}

				// For standalone use
				if (destDir.equals("")) {
					log.warning("loaderDir not defined - move skipped");
					break;
				}

				if (!destDir.endsWith(File.separator)) {
					destDir += File.separator;
				}

				// outputfile: meastype_adapterDate_dataDate.txt

				final String typeName = parseFileName(outputFile.getName(), "(.+)_.+_.+_.+");
				final String firstDateID = parseFileName(outputFile.getName(), ".+_.+_.+_(.+)");
				
				// For Version implementation as part of Backup&Restore feature
				final String buildNo = "_b" + parseFileName(dfCache.getFormatWithFolderName(typeName).getDataFormatID(), ".*\\(\\((.+)\\)\\).*");
				if(buildNo.equalsIgnoreCase("")){
					log.info("The data format id for " +typeName+ " is: " + dfCache.getFormatWithFolderName(typeName).getDataFormatID() + " and data format object is: " + dfCache.getFormatWithFolderName(typeName).toString());
				}
				log.finest("BUILD No. of TP - " + buildNo);

				if (NoOfDir > 0) { // If not a single FSA Server
					/*
					 * destDir which is loaderDir is set in the parse action. loaderDir=${ETLDATA_DIR} =
					 * /eniq/data/etldata Change /eniq/data/etldata to /eniq/data/_etldata_ for RAW (multiple file
					 * systems). Everything points to /eniq/data/etldata_/XX
					 */
					destDir = System.getProperty(CommonUtils.EVENTS_ETLDATA_DIR, CommonUtils.EVENTS_ETLDATA_DIR_VAL)
							+ File.separator;
					destDir += preAppend + folderNumber + File.separator;
				}

				destDir += typeName.toLowerCase() + File.separator;

				log.fine("Destination Directory is :" + destDir);

				String dest_last = typeName + "_" + System.currentTimeMillis();
				// Removed usage of nanotime as it seems not to work on every machine.
				// dest_last += System.nanoTime();
				// dest_last += "_";

				int index = 0;
				String dateInfo = "_";
				if ((firstDateID == null) || firstDateID.equalsIgnoreCase("null")) {
					log.finest("DATE_ID column was not found from file \"" + outputFile.getName()
							+ "\". Using parsing date.");
					dateInfo += sdf.format(new Date());
				} else {
					log.finest("Moved. First DATE_ID was " + firstDateID);
					dateInfo += firstDateID;
				}

				final String ending;
				if (outputFormat.equals("1")) { // If output is to be binary give
												// .binary extension
					ending = ".binary"; // ..other wise give .txt extension for ASCII
										// output.
					log.finest("Output is Binary. File extension will be .binary");
				} else if (outputFormat.equals("0") || outputFormat.equals("")) {
					ending = ".txt";
					log.finest("Output is ASCII. File extension will be .txt");
				} else {
					throw new Exception("outputFormat property is unexpected value: "
							+ conf.getProperty("outputFormat") + ". Measurement files will not be moved to loader!!");
				}

				String targetFileName = destDir + "raw" + File.separator + dest_last + buildNo + dateInfo + ending;

				final String targetDirName = destDir + "raw";

				final File targetDir = new File(targetDirName);

				if (!targetDir.exists()) {
					// log.finer("Directory " + targetDir +
					// " doesn't exist. Being created now.");
					log.warning("Directory " + targetDir + " doesn't exist.");
					// targetDir.mkdirs();
				}

				File targetFile = new File(targetFileName);

				while (targetFile.exists()) {
					if (index > 999) {
						dest_last = typeName + "_" + System.currentTimeMillis();
						targetFileName = destDir + "raw" + File.separator + dest_last + buildNo + dateInfo + ending;
						index = 0;
					} else {
						targetFileName = destDir + "raw" + File.separator + dest_last + index + buildNo + dateInfo + ending;
					}
					targetFile = new File(targetFileName);
					index++;
					log.finest("Trying to find the next unique target filename  " + targetFileName);
				}
				log.finer("Moving file " + outputFile.getName() + " to " + targetFileName);

	 boolean moveSuccess = false;
				 
				 //For TR HU57021
             try {
				 moveSuccess = outputFile.renameTo(targetFile);
				if (moveSuccess) {
					measfileMoveCount++;
					log.finest("measfileMoveCount inside moveFilesToLoader()--->" + measfileMoveCount); }
             }
				
				catch (final Exception e) {
					log.log(Level.WARNING, "renameTo failed. Moving with memory copy, deleting the temporary file", e);
				

					log.severe("Parser was forced to move outDir -> loaderDir via memory copy. Check configuration!");
					log.severe("outDir and loaderDir directories should be in same partition or performance is ruined.");

					
                       try{    
						final InputStream in = new FileInputStream(outputFile);
						final OutputStream out = new FileOutputStream(targetFile);

						final byte[] buf = new byte[1024];
						int len;
						while ((len = in.read(buf)) > 0) {
							out.write(buf, 0, len);
						}
						in.close();
						out.close();

						outputFile.delete();
                       }
                       catch (final Exception e1) {
   						log.log(Level.WARNING, "Move with memory copy failed, deleting the temporary file", e1);
   						outputFile.delete();
   					}
				}
             
          
             if (!moveSuccess) {


					log.finer("renameTo failed. Moving with memory copy");
					log.severe("Parser was forced to move outDir -> loaderDir via memory copy. Check configuration!");
					log.severe("outDir and loaderDir directories should be in same partition or performance is ruined.");

					try {

						final InputStream in = new FileInputStream(outputFile);
						final OutputStream out = new FileOutputStream(targetFile);

						final byte[] buf = new byte[1024];
						int len;
						while ((len = in.read(buf)) > 0) {
							out.write(buf, 0, len);
						}
						in.close();
						out.close();

						outputFile.delete();
						measfileMoveCount++;


					} catch (final Exception e) {
						
						log.log(Level.WARNING, "Move with memory copy failed, deleting the temporary file", e);
						outputFile.delete();
						
					}

				}
			}
			}else
			{
				log.finest("Directory "+outDir +"  doesnot exists");
			}

		}
		if (measfileMoveCount == totalNumFiles) {// makes sure that all files have been moved successfully	
			log.finest("Number of mesurement files moved -->" + measfileMoveCount);

			handleSourceFiles();

		} else {
			log.warning("The sourcefile deletion is not complete as all the "
				+ "files are not moved successfully to Loader directory.");
			log.warning("measfileMoveCount-->" + measfileMoveCount);
			log.warning("number of outFile-->" + totalNumFiles);
		}
	}

	/**
	 * Delete or hides files depending of property settings.
	 * 
	 * @throws Exception
	 */
	public void handleSourceFiles() throws Exception, FileNotFoundException {
		final String parsername = conf.getProperty("parserType");
		final String aftParseAction = conf.getProperty("afterParseAction", "");
		final Iterator<SourceFile> listIterator = sourceFileList.iterator();
		if (!(parsername.equalsIgnoreCase("alarm"))) {
			
			SourceFile sfile = null;
			
			if (aftParseAction.equals("delete")) {
					while (listIterator.hasNext()) {
						sfile = listIterator.next();
						if (sfile != null) {
							
							sfile.delete();
							addToProcessedList(sfile);
						}
					}
			} else if (aftParseAction.equals("hide")) {
					while (listIterator.hasNext()) {
						sfile = listIterator.next();
						if (sfile != null) {
							
							sfile.hide(rollingHideDir);
							addToProcessedList(sfile);
						}
					}
			}
			sourceFileList.clear();
		}
	}

	private void addToProcessedList(SourceFile sfile) throws FileNotFoundException {
		if ((checker != null) && !sfile.getErrorFlag()) {
			checker.addToProcessed(sfile.getName(),
					checker.getProcessedDir(conf.getProperty("ProcessedFiles.processedDir"))
							+ File.separator + conf.getProperty("interfaceName"));
			log.finest("sourceFile added to the processedList-->" + sfile);
		}
	}

	/**int calculateMaxWorkerCount() {
		// EEIKBE: Query the ExecutionSlots to see how many Adapter slots there are
		// when the number is retrieved get 60% of it and this is the new maxWorkers
		// value.
		final Share share = Share.instance();
		final ExecutionSlotProfileHandler executionSlotProfileHandler = (ExecutionSlotProfileHandler) share
				.get("executionSlotProfileObject");
		final int numberOfAdapterSlots = executionSlotProfileHandler.getNumberOfAdapterSlots();
		if (numberOfAdapterSlots <= 1) {
			return 1;
		}
		final double calculatedWorkerNumber = (numberOfAdapterSlots * 0.6); // 60%
		return (int) Math.floor(calculatedWorkerNumber);// round down to then next
														// closest integer
	}*/

	/**
	 * Determines amout of workers and creates them.
	 */
	private Map<String, Parser> createParserWorkers() throws Exception {

		int maxWorkers = 29;
		//int maxWorkers = calculateMaxWorkerCount();

		try {
			String smaxWorkers = conf.getProperty("workers.maxWorkers", null);
			if (smaxWorkers == null) {
				smaxWorkers = StaticProperties.getProperty("Parsers.workers.maxWorkers");
				log.fine("static.properties - Parsers.workers.maxWorkers: " + smaxWorkers
						+ " overrides caluclated Number of workers: " + maxWorkers);
			} else {
				log.fine("properties - workers.maxWorkers: " + smaxWorkers
						+ " overrides caluclated Number of workers: " + maxWorkers);
			}
			maxWorkers = Integer.parseInt(smaxWorkers);
		} catch (final Exception e) {
		}

		log.fine("Maximum worker count " + maxWorkers);

		int filesPerWorker = 100;

		try {
			String sfilePerWorker = conf.getProperty("workers.filesPerWorker", null);
			if (sfilePerWorker == null) {
				sfilePerWorker = StaticProperties.getProperty("Parsers.workers.filesPerWorker");
			}
			filesPerWorker = Integer.parseInt(sfilePerWorker);
		} catch (final Exception e) {
		}

		log.fine("Files per worker " + filesPerWorker);

		int mbytesPerWorker = 50;

		try {
			String smbPerWorker = conf.getProperty("workers.megabytesPerWorker", null);
			if (smbPerWorker == null) {
				smbPerWorker = StaticProperties.getProperty("Parsers.workers.megabytesPerWorker");
			}
			mbytesPerWorker = Integer.parseInt(smbPerWorker);
		} catch (final Exception e) {
		}

		log.fine("Megabytes per worker " + mbytesPerWorker);
		

		int workercount = ((fileList.size() - 1) / filesPerWorker) + 1;

		log.finest("By filecount produced " + workercount + " workers");

		if (workercount > maxWorkers) {
			log.fine("maxWorkers reached");
			workercount = maxWorkers;
		} else {
			final int mbworkers = ((fileListSize() - 1) / mbytesPerWorker) + 1;

			log.finest("By filesize produced " + mbworkers + " workers");

			if (mbworkers > workercount) {
				workercount = mbworkers;
			}

			if (workercount > maxWorkers) {
				log.fine("maxWorkers reached");
				workercount = maxWorkers;
			}
			if(enginerobustflag.equalsIgnoreCase("true")){
				log.info("worker generation for engine improvement. ");
				final Runtime rt = Runtime.getRuntime();
				final int avgfilesize=(fileListSize()*1024*1024)/(fileList.size());
				
				if ((avgfilesize * 29 > rt.freeMemory()*.1) || (avgfilesize > 10*1024*1024)){
					if (((rt.freeMemory() < (rt.totalMemory() * .2 )) || (rt.freeMemory() < (rt.maxMemory() * .1 ))) || ((fileListSize() * 8 * 1024 * 1024)> rt.freeMemory())){
					
	
					final int heapworker=(int) ((rt.freeMemory() * .5 ) / (fileListSize() * 1024 * 1024));
					log.fine("heap worker " + workercount + "  workers." );
	
					if ( workercount > heapworker)
						workercount = heapworker;				
					}
					int minworkercount=((int) ((rt.freeMemory())/(avgfilesize*29)))+1;
					if ((minworkercount < 1)||(workercount < 1)){
						minworkercount=2;
						workercount=2;
					}
					if ( workercount >= minworkercount){
						workercount = minworkercount;
						
					}
					
					log.info("Based on Free memory, Creating " + workercount + "  workers." );
				}
			}
		}
		log.info("Creating " + workercount + " workers");

		final Map<String, Parser> workerList = new HashMap<String, Parser>();
		for (int i = 0; i < workercount; i++) {
			log.finer("Creating worker parser.");
			final Parser parser = createParser();
			log.log(Level.FINE, "Parser " + parser.getClass().getName() + " created. Starting parser init...");
			final String workerName = "w" + i + System.currentTimeMillis();
			parser.init(this, techPack, set_type, set_name, workerName);
			log.log(Level.FINE, "Parser " + "w" + System.currentTimeMillis()
					+ " initialized... Adding parser to workerList...");
			workerList.put(workerName, parser);
		}
		return workerList;
	}

	/**
	 * Factory method for creating technology specific parser implementation.
	 * 
	 * @param sf
	 *            SourceFile to be parsed.
	 * @return Parser to do it.
	 * @throws Exception
	 *             in case of failure.
	 */
	private Parser createParser() throws Exception {

		final String name = conf.getProperty("parserType");
		String implClass = null;

		log.finer("Trying to find parserImpl class with parser name: " + name);

		if (name.equalsIgnoreCase("alluascii")) {
			implClass = "com.ericsson.eniq.etl.alcatellucent.parser.AlLuAsciiBlockParser";

		} else if (name.equalsIgnoreCase("allubin")) {
			implClass = "com.ericsson.eniq.etl.alcatellucent.parser.AlLuBinParser";

		} else if (name.equalsIgnoreCase("alluasciilist")) {
			implClass = "com.ericsson.eniq.etl.alcatellucent.parser.AlLuAsciiListParser";

		} else if (name.equalsIgnoreCase("e_fnr")) {
			implClass = "com.ericsson.eniq.etl.ebinary.EFNRParser";

		} else if (name.equalsIgnoreCase("e_hlr")) {
			implClass = "com.ericsson.eniq.etl.ebinary.EHLRParser";

		} else if (name.equalsIgnoreCase("alarm")) {
			implClass = "com.distocraft.dc5000.etl.alarm.AlarmParser";

		} else if (name.equalsIgnoreCase("ascii")) {
			implClass = "com.distocraft.dc5000.etl.ascii.ASCIIParser";

		} else if (name.equalsIgnoreCase("eascii")) {
			implClass = "com.distocraft.dc5000.etl.eascii.EASCIIParser";

		} else if (name.equalsIgnoreCase("nascii")) {
			implClass = "com.distocraft.dc5000.etl.nascii.NASCIIParser";
			
		} else if (name.equalsIgnoreCase("mrr")) {
			implClass = "com.ericsson.eniq.etl.mrr.MRRParser";

		} else if (name.equalsIgnoreCase("eniqasn1")) {
			implClass = "com.ericsson.eniq.etl.asn1.ASN1Parser";

		} else if (name.equalsIgnoreCase("mdc")) {
			implClass = "com.distocraft.dc5000.etl.MDC.MDCParser";

		} else if (name.equalsIgnoreCase("nossdb")) {
			implClass = "com.distocraft.dc5000.etl.nossdb.NOSSDBParser";

		} else if (name.equalsIgnoreCase("omes")) {
			implClass = "com.distocraft.dc5000.etl.omes.OMESParser";

		} else if (name.equalsIgnoreCase("omes2")) {
			implClass = "com.distocraft.dc5000.etl.omes2.OMES2Parser";

		} else if (name.equalsIgnoreCase("raml")) {
			implClass = "com.distocraft.dc5000.etl.raml.RAMLParser";

		} else if (name.equalsIgnoreCase("separator")) {
			implClass = "com.distocraft.dc5000.etl.separartor.SeparatorParser";

		} else if (name.equalsIgnoreCase("stfiop")) {
			implClass = "com.distocraft.dc5000.etl.stfiop.STFIOPParser";

		} else if (name.equalsIgnoreCase("xml")) {
			implClass = "com.distocraft.dc5000.etl.xml.XMLParser";

		} else if (name.equalsIgnoreCase("csexport")) {
			implClass = "com.distocraft.dc5000.etl.csexport.CSExportParser";

		} else if (name.equalsIgnoreCase("ct")) {
			implClass = "com.ericsson.eniq.etl.ct.CTParser";

		} else if (name.equalsIgnoreCase("spf")) {
			implClass = "com.ericsson.eniq.etl.spf.SPFParser";

		} else if (name.equalsIgnoreCase("unittest")) {
			implClass = "com.ericsson.eniq.etl.parser.unittests.UnittestParser";

		} else if (name.equalsIgnoreCase("sasn")) {
			implClass = "com.ericsson.eniq.etl.sasn.SASNParser";

		} else if (name.equalsIgnoreCase("3gpp32435")) {
			implClass = "com.distocraft.dc5000.etl.xml3GPP32435.Xml3GPP32435Parser";

		} else if (name.equalsIgnoreCase("ebs")) {
			implClass = "com.distocraft.dc5000.etl.ebs.EBSParser";

		} else if (name.equalsIgnoreCase("axd")) {
			implClass = "com.ericsson.eniq.etl.asn1.AXDParser";

		} else if (name.equalsIgnoreCase("redback")) {
			implClass = "com.ericsson.eniq.etl.RedbackParser.RedbackParser";

		} else if (name.equalsIgnoreCase("bcd")) {
			implClass = "com.ericsson.eniq.etl.bcd.BCDParser";

		} else if (name.equalsIgnoreCase("iptnmsPS")) {
			implClass = "com.ericsson.eniq.etl.iptnmsPS.IptnmsPSParser";

		} else if (name.equalsIgnoreCase("iptnmsCS")) {
			implClass = "com.ericsson.eniq.etl.iptnmsCS.IptnmsCSParser";

		} else if (name.equalsIgnoreCase("minilink")) {
			implClass = "com.distocraft.dc5000.etl.mlxml.mlXmlParser";

		} else if (name.equalsIgnoreCase("wifi")) {
			implClass = "com.ericsson.wifi.ewmnbi.WIFIParser";

		} else if (name.equalsIgnoreCase("wifiinventory")) {
			implClass = "com.ericsson.wifi.ewmnbi.WIFIInventoryParser";
			
		} else if (name.equalsIgnoreCase("twampM")) {
			implClass = "com.ericsson.eniq.etl.twampM.twampM";	
			
		} else if (name.equalsIgnoreCase("twampPT")) {
			implClass = "com.ericsson.eniq.etl.twampPT.twampPT";
			
		} else if (name.equalsIgnoreCase("twampST")) {
			implClass = "com.ericsson.eniq.etl.twampST.twampST";
			
		} else if (name.equalsIgnoreCase("json")) {
			implClass = "com.ericsson.eniq.etl.json.JsonParser";
		} else {
			log.fine("parser name not found: " + name);
			implClass = name;
		}

		log.finer("Trying to instantiate parserImpl class: " + implClass);

		final Parser parser = (Parser) (Class.forName(implClass).newInstance());

		log.finer("Parser instance successfully constructed");

		return parser;

	}

	/**
	 * Checks that needed directory structure exists. If not tries to create missing directories.
	 * 
	 * @throws Exception
	 *             if error(s) found and unable to fix them
	 */
	private boolean checkDirectories() throws Exception {

		final long start = System.currentTimeMillis();

		String baseDir = resolveDirVariable(conf.getProperty("baseDir"));

		if (!baseDir.endsWith(File.separator)) {
			baseDir += File.separator;
		}

		final File bDirFile = new File(baseDir);

		if (!bDirFile.exists()) {
			log.fine("Base directory doesn't exists. Creating baseDir " + baseDir);
			if (!bDirFile.mkdirs()) {
				log.fine("Unable to create baseDir " + baseDir);
				return false;
			}
		}
		
		if (!bDirFile.isDirectory() || !bDirFile.canWrite()) {
			log.fine("Unable to access baseDir " + baseDir);
			return false;
			// throw new Exception("Unable to access baseDir " + baseDir);
		}

		log.fine("base dir is: " + baseDir);

		File inDir;
		try {
			final String directory = resolveDirVariable(conf.getProperty("inDir"));
			inDir = new File(directory); // explicit in-directory ?
		} catch (final Exception e) {
			inDir = new File(baseDir + "in");
		}

		// if (!inDir.exists())
		// inDir.mkdirs();

		if (!inDir.isDirectory() || !inDir.canWrite()) {
			log.fine("Unable to access inDir " + inDir);
			return false;
			// throw new Exception("Unable to access inDir " + inDir);
		}

		log.fine("input dir is: " + inDir);

		File outDir;
		try {
			final String directory = resolveDirVariable(conf.getProperty("outDir"));
			outDir = new File(directory);
		} catch (final Exception e) {
			outDir = new File(baseDir + "out");
		}

		if (!outDir.exists()) {
			outDir.mkdirs();
		}

		if (!outDir.isDirectory() || !outDir.canWrite()) {
			throw new Exception("Unable to access outDir " + outDir);
		}

		log.fine("output dir is: " + outDir);

		// --- Directories that follow only the basedir ---

		int archiveLength = 168; // 168 hours = 1 week

		try {
			archiveLength = Integer.parseInt(conf.getProperty("archivePeriod", ""));
		} catch (final NumberFormatException e) {
			log.config("archivePeriod not defined. Assuming 1 week.");
		}

		archiveLength = archiveLength * 60 * 60 * 1000; // Same in milliseconds

		final String archiveStampFormat = conf.getProperty("archiveStampFormat", "yyyyMMddHHmm");

		final SimpleDateFormat archSDF = new SimpleDateFormat(archiveStampFormat);

		final FileFilter ff = new FileFilter() {

			@Override
			public boolean accept(final File fil) {
				final String name = fil.getName();
				final int ix = name.lastIndexOf("_");

				return (fil.isDirectory() && name.startsWith("_") && (ix > 0) && (ix < (name.length() - 2)));
			}
		};

		// ---

		final File archiveBaseDir = new File(baseDir + "archive");

		archiveDir = checkRotationalDirectory(archiveBaseDir, ff, archSDF, archiveLength);

		log.fine("archive dir is: " + archiveDir);

		final File failedBaseDir = new File(baseDir + "failed");

		failedDir = checkRotationalDirectory(failedBaseDir, ff, archSDF, archiveLength);

		log.fine("failed dir is: " + failedDir);

		final File doubleBaseDir = new File(baseDir + "double");

		doubleDir = checkRotationalDirectory(doubleBaseDir, ff, archSDF, archiveLength);

		log.fine("double dir is: " + doubleDir);

		performanceLog.finest("Check directories completed " + (System.currentTimeMillis() - start) + " ms");

		return true;

	}

	private File checkRotationalDirectory(final File baseRotationDir, final FileFilter ff,
			final SimpleDateFormat archSDF, final int archiveLength) throws Exception {

		File retDir = null;
		
		if (!baseRotationDir.exists()) {
			baseRotationDir.mkdirs();
		}

		if (!baseRotationDir.isDirectory() || !baseRotationDir.canWrite()) {
			throw new Exception("Unable to access " + baseRotationDir);
		}

		final File[] incomplete = baseRotationDir.listFiles(ff);

		log.fine("Found " + incomplete.length + " unfinished directories");

		// Go through existing incomplete directories
		for (final File element : incomplete) {
			final String name = element.getName();
			final int ix = name.lastIndexOf("_");

			final Date lastTime = archSDF.parse(name.substring(ix + 1));
			if (System.currentTimeMillis() > lastTime.getTime()) {
				try {
					element.renameTo(new File(element.getParent(), element.getName().substring(1)));
					log.fine("Incomplete dir " + element.getName() + " now finished");
				} catch (final Exception e) {
					log.log(Level.WARNING, "Rename failed", e);
				}
			} else {
				log.fine("Found relevant existing directory");
				retDir = element;
			}

		}

		if (retDir == null) { // archive not exits. Create new archive
			final long now = System.currentTimeMillis();
			retDir = new File(baseRotationDir, "_" + archSDF.format(new Date(now)) + "_"
					+ archSDF.format(new Date(now + archiveLength)));

			log.fine("Created new dir " + retDir.getName());
		}

		if (!retDir.exists()) {
			retDir.mkdirs();
		}

		if (!retDir.isDirectory() || !retDir.canWrite()) {
			//code changes for TR HR37121
			log.fine(retDir+" has been overloaded. ");
			retDir.setWritable(true);
			if(!retDir.canWrite())
			{
			throw new Exception("Unable to access dir " + retDir);
		}
			//code changes for TR HR37121
			}

		return retDir;

	}

	/**
	 * Extracts a substring from given string based on given regExp
	 * 
	 */
	public String parseFileName(final String str, final String regExp) {

		final Pattern pattern = Pattern.compile(regExp);
		final Matcher matcher = pattern.matcher(str);

		if (matcher.matches()) {
			final String result = matcher.group(1);
			log.finest(" regExp (" + regExp + ") found from " + str + "  :" + result);
			return result;
		} else {
			log.warning("String " + str + " doesn't match defined regExp " + regExp);
		}

		return "";

	}

	/**
	 * Resolves ${VARIABLE} paths against environment variables.
	 */
	public static String resolveDirVariable(String directory) {

		if (directory == null) {
			return null;
		}

		if (directory.indexOf("${") >= 0) {
			final int sti = directory.indexOf("${");
			final int eni = directory.indexOf("}", sti);

			if (eni >= 0) {
				final String variable = directory.substring(sti + 2, eni);
				final String val = System.getProperty(variable);
				final String result = directory.substring(0, sti) + val + directory.substring(eni + 1);
				directory = result;
			}
		}

		return directory;
	}

	private void handlePendingTempFiles(String destDir, final String techPack, final int handleCase) {

		if (!destDir.endsWith(File.separator)) {
			destDir += File.separator;
		}

		destDir = Main.resolveDirVariable(destDir);
		destDir += techPack;
		final File ddir;

		log.finest("Handling temporary files in directory: " + destDir);

		try {
			switch (handleCase) {

			case 0:
				// do nothing
				log.finest("Handling temporary files: doing nothing.");
				break;

			case 1:
				// delete files
				log.finest("Handling temporary files: deleting the files.");
				ddir = new File(destDir);
				deleteTempFiles(ddir);
				break;

			case 2:
				// move files to loader
				log.finest("Handling temporary files: moving the files to loader.");
				moveFilesToLoader();
				break;

			case 3:
				// remove last lines
				log.finest("Handling temporary files: removing last lines from the files.");
				ddir = new File(destDir);
				removeLastLinesFromTempFiles(ddir);
				break;

			}

		} catch (final Exception e) {
			log.log(Level.WARNING, "Parser failed to exception, when handling temporary files.", e);
		}

	}

	private void deleteTempFiles(final File destDir) {
		if (destDir.exists()) {
			final File[] files = destDir.listFiles();
			for (final File file : files) {
				file.delete();
			}
		}
	}

	private void removeLastLinesFromTempFiles(final File destDir) throws Exception {
		if (destDir.exists()) {

			final FilenameFilter filter = new FilenameFilter() {

				@Override
				public boolean accept(final File dir, final String name) {
					return !name.endsWith(WORKING_COPY_SUFFIX);
				}
			};

			final File[] tempFiles = destDir.listFiles(filter);
			if (outputFormat.equals("1")) {
				final String interfaceName = conf.getProperty("interfaceName");

				for (final File tempFile : tempFiles) {
					log.finest("Removing partial (last) row from binary file: " + tempFile.getAbsolutePath());
					removeLastLineBinary(tempFile, interfaceName);
				}
			} else if (outputFormat.equals("0") || outputFormat.equals("")) {
				for (final File tempFile : tempFiles) {
					log.finest("Removing last line from ASCII file: " + tempFile.getAbsolutePath());
					removeLastLine(tempFile);
				}
			} else {
				throw new Exception("outputFormat property is unexpected value: " + outputFormat
						+ ". Measurement files will not have partial (last) row removed.");
			}

		}
	}

	private void removeLastLine(final File tempFile) {
		BufferedReader reader = null;
		BufferedWriter writer = null;

		try {
			final File outFile = new File(tempFile.getAbsolutePath() + WORKING_COPY_SUFFIX);
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(tempFile)));
			writer = new BufferedWriter(new FileWriter(outFile, false));

			// read lines (without last line) from original file and write them into
			// working copy
			String prevLine = reader.readLine();
			String nextLine = null;
			while (prevLine != null) {
				nextLine = reader.readLine();
				if (nextLine != null) {
					writer.write(prevLine + "\n");
				}
				prevLine = nextLine;
			}

			// close files
			writer.close();
			reader.close();

			// get a filename from where the last row is to be removed and delete the
			// file
			final String path = tempFile.getAbsolutePath();
			tempFile.delete();

			// rename working copy file (from where last row is deleted) to original
			// filename
			final File newFile = new File(path);
			outFile.renameTo(newFile);

		} catch (final Exception e) {
			log.warning("Error occured in removing last line from adapters temporary file.");

		} finally {
			try {
				if (null != writer) {
					writer.close();
				}
			} catch (final Exception e) {
			}

			try {
				if (null != reader) {
					reader.close();
				}
			} catch (final Exception e) {
			}
		}

	}

	private void removeLastLineBinary(final File tempFile, final String interfaceName) throws Exception {

		// int asdf = (int) tempFile.length();
		// Get data format cache

		// Extract the tagID (measurement name) from file name
		final String filename = tempFile.getName();

		// Get the data format of measurement type (data format of this file)
		final String[] filenameSplit = filename.split("_");
		String measNameFromFileName = new String();
		for (int i = 0; i < (filenameSplit.length - 3); i++) {
			measNameFromFileName += filenameSplit[i];
			if (i < (filenameSplit.length - 4)) {
				measNameFromFileName += "_";
			}
		}

		final DFormat dataformat = dfCache.getFormatWithFolderName(measNameFromFileName);
		// Get the sum of the data sizes for this measurement type.
		final int sizeOfRow = getRowByteSize(dataformat);

		final int fileSize = (int) tempFile.length();

		// If the last row is partial then this is a rounded down value.
		final int numOfCompleteRows = fileSize / sizeOfRow;

		final int numOfBytesToCopy = numOfCompleteRows * sizeOfRow;

		BufferedInputStream breader = null;
		BufferedOutputStream bwriter = null;
		// Copy all the complete rows from the measurement file to a working copy
		// file, delete the original
		// file, and then name the new file the same as the original
		try {
			final File outFile = new File(tempFile.getAbsolutePath() + WORKING_COPY_SUFFIX);
			breader = new BufferedInputStream(new FileInputStream(tempFile));
			bwriter = new BufferedOutputStream(new FileOutputStream(outFile, true));

			final byte[] bytesFromFile = new byte[numOfBytesToCopy]; // Fill this array with
			// all the bytes in the
			// file
			breader.read(bytesFromFile);
			bwriter.write(bytesFromFile);

			// close files
			bwriter.close();
			breader.close();

			// Delete the original file
			tempFile.delete();

			// Rename working copy file to name of original file
			outFile.renameTo(tempFile);

		} catch (final Exception e) {
			log.warning("Error occured in removing last line from adapters temporary file.");

		} finally {
			try {
				if (null != bwriter) {
					bwriter.close();
				}
			} catch (final Exception e) {
			}

			try {
				if (null != breader) {
					breader.close();
				}
			} catch (final Exception e) {
			}
		}
	}

	/**
	 * Gets the size in bytes of a row of data (as output to measurement file) for a measurement type with submitted
	 * dataformat.
	 * 
	 * @param dataformat
	 * @return
	 * @throws Exception
	 */
	public static int getRowByteSize(final DFormat dataformat) throws Exception {
		int sizeOfRow = 0;
		final Iterator<DItem> iterator = dataformat.getDItems();
		while (iterator.hasNext()) {
			final DItem mData = iterator.next();

			final String dataType = mData.getDataType();
			final int dataSize = mData.getDataSize();

			if (dataType.equalsIgnoreCase("bit")) {
				sizeOfRow += 2;
			} else if (dataType.equalsIgnoreCase("tinyint")) {
				sizeOfRow += 2;
			} else if (dataType.equalsIgnoreCase("smallint")) {
				sizeOfRow += 3;
			} else if (dataType.equalsIgnoreCase("int")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("integer")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("unsigned int")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("bigint")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("unsigned bigint")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("float")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("char")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("varchar")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("binary")) {
				sizeOfRow += dataSize + 1;
			} else if (dataType.equalsIgnoreCase("varbinary")) {
				sizeOfRow += 0;
			} else if (dataType.equalsIgnoreCase("date")) {
				sizeOfRow += 5;
			} else if (dataType.equalsIgnoreCase("time") || dataType.equalsIgnoreCase("datetime")
					|| dataType.equalsIgnoreCase("timestamp")) {
				sizeOfRow += 9;
			} else if (dataType.equalsIgnoreCase("numeric") || dataType.equalsIgnoreCase("decimal")) {
				if (dataSize <= 4) {
					sizeOfRow += 3;
				} else if ((dataSize >= 5) && (dataSize <= 9)) {
					sizeOfRow += 5;
				} else if ((dataSize >= 10) && (dataSize <= 18)) {
					sizeOfRow += 9;
				} else {
					sizeOfRow += 71;
				}
			} else {
				throw new Exception("Unsupported dataType found in dataformat: " + dataType + ".");
			}

		}
		return sizeOfRow;
	}

	/**
	 * 
	 * Configures memory usage restrictions (memory consumption for concurrent worker limitations). Returns true if set
	 * name matches with some configured regular expression.
	 * 
	 */
	private boolean configureMemoryUsageRestrictions(final Map<String, String> regexpsForWorkerLimitations,
			final Map<String, Integer> memoryUsageFactors) {
		boolean returnValue = false;

		if ((regexpsForWorkerLimitations == null) || (memoryUsageFactors == null)) {
			return returnValue;
		}

		final Iterator<String> iter = regexpsForWorkerLimitations.keySet().iterator();
		String key = "";
		String regexp = "";
		final int memFactorsSize = memoryUsageFactors.size();
		final int regexpsSize = regexpsForWorkerLimitations.size();

		while (iter.hasNext() && !returnValue) {
			key = iter.next();
			regexp = regexpsForWorkerLimitations.get(key);
			returnValue = setNameMatchesWithRegexp(set_name, regexp);
		}

		// if configured regular expression matched with set name let us configure
		// the memoryUsageFactor and regexp to be used

		if (returnValue && (memFactorsSize == regexpsSize)) {
			try {
				memoryUsageFactor = memoryUsageFactors.get(key);
				regexpForWorkerLimit = regexp;
			} catch (final Exception e) {
				log.warning("No memory usage factor found for key: " + key + e.toString());
				returnValue = false;
			}
		} else {
			returnValue = false;
		}

		return returnValue;
	}

	/**
	 * 
	 * Checks that does the set name match with given regular expression.
	 * 
	 */
	private boolean setNameMatchesWithRegexp(final String setName, final String regexp) {
		boolean returnValue = false;
		try {
			final Pattern pat = Pattern.compile(regexp);
			final Matcher mat = pat.matcher(setName);
			returnValue = mat.find();
		} catch (final Exception e) {
			log.log(Level.WARNING, "Set limitation regexp matching failed.", e);
		}
		return returnValue;
	}

	public static void main(final String[] args) {

		try {

			if (args.length <= 0) {
				System.err.println("\"source\" must be defined as argument");
				return;
			}

			final Properties props = new com.distocraft.dc5000.common.Properties(args[0], new Hashtable());

			String confDir = System.getProperty("dc5000.config.directory");
			if (!confDir.endsWith(File.separator)) {
				confDir += File.separator;
			}

			StaticProperties.reload();

			//final Main m = new Main(props, "x", "x", "x", null, null, new EngineCom());
			//m.parse();

		} catch (final Exception e) {
			e.printStackTrace();
		}

	}

	public int getMemoryConsumptionMB() {
		if (internalWorker instanceof MemoryRestrictedParser) {
			return ((MemoryRestrictedParser) internalWorker).memoryConsumptionMB();
		} else {
			return 0;
		}
	}

	public FileInformation createFileInformation(final File file, final long fileSize, final int memoryConsumptionB) {
		return new FileInformation(file, fileSize, memoryConsumptionB);
	}

	public class FileInformation {

		public File file;

		public long fileSizeB;

		public int memoryConsumptionMB;
		
		public ZipEntry zipEntry;
		
		public boolean isZipEntry = false;

		public boolean isZipEntry() {
			return isZipEntry;
		}

		public void setZipEntry(boolean isZipEntry) {
			this.isZipEntry = isZipEntry;
		}

		public FileInformation(final File file, final long fileSize, final int memoryConsumptionB) {
			this.file = file;
			this.fileSizeB = fileSize;
			this.memoryConsumptionMB = (memoryConsumptionB == 0 ? 0 : (int) Math.ceil(memoryConsumptionB / 1024 / 1024));
		}
		
		public FileInformation(final File file, ZipEntry zipEntry , final long fileSize, final int memoryConsumptionB) {
			this.file = file;
			this.zipEntry = zipEntry;
			this.isZipEntry = true;
			this.fileSizeB = fileSize;
			this.memoryConsumptionMB = (memoryConsumptionB == 0 ? 0 : (int) Math.ceil(memoryConsumptionB / 1024 / 1024));
		}
	}
	
	/*Subfunction for putting items in a comma separated string into a list */
	public List strToList(final String str) {

	    final ArrayList list = new ArrayList();

	    if (str != null) {

	      // list all items in string
	      final StringTokenizer tokens = new StringTokenizer(((String) str), ",");
	      while (tokens.hasMoreTokens()) {
	        list.add(tokens.nextToken());
	      }
	    }

	    return list;
	}

	private static class FileInformationComparator implements Comparator {

		@Override
		public int compare(final Object o1, final Object o2) {
			if (o1 == o2) {
				return 0;
			}

			final FileInformation fi1 = (FileInformation) o1;
			final FileInformation fi2 = (FileInformation) o2;

			return ((Long) fi2.fileSizeB).compareTo((fi1.fileSizeB));
		}
	}
	private  String checkMultiBlade()
	  {
	    try
	    {
	      String command = "";
	      List localList = DBUsersGet.getMetaDatabases("dcuser", "engine");
	        localList = DBUsersGet.getMetaDatabases("dcuser", "engine");
	        if (localList.isEmpty()) {
	          throw new Exception("Could not find an entry for dcuser:engine in repdb! (was is added?)");
	        }
	      String password = ((Meta_databases)localList.get(0)).getPassword();
	      command = "cat /eniq/installation/config/installed_server_type";
	      String result = RemoteExecutor.executeComand("dcuser", password, "engine", command);
	      if(result.contains("stats") && !result.trim().equalsIgnoreCase("eniq_stats")){
	         return "multiblade";
	      }else{
	    	  return "blade";
	      }

	    }
	    catch (JSchException localJSchException)
	    {
	      localJSchException.printStackTrace();
	    }
	    catch (Exception localException)
	    {
	      localException.printStackTrace();
	    }
		return "blade";
	  }
}