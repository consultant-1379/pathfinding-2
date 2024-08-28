package com.ericsson.eniq.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import static com.ericsson.eniq.parser.Constants.*;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ericsson.eniq.parser.cache.DFormat;
import com.ericsson.eniq.parser.cache.DataFormatCache;
import com.ericsson.eniq.parser.sink.ISink;

public class MeasurementFileFactory {

	private static DataFormatCache dFCache;
	private static Map<String, IMeasurementFile> textMeasurementFileMap = new ConcurrentHashMap<>();
	private static Map<String, IMeasurementFile> binMeasurementFileMap = new ConcurrentHashMap<>();
	private static Map<String, IMeasurementFile> parquetMeasurementFileMap = new ConcurrentHashMap<>();

	private static BlockingQueue<Data> queue = new LinkedBlockingQueue<>();
	
	private static final Object LOCK = new Object();
	
	private static final Map<Integer, WriteTask> writeThreads = new HashMap<>();
	
	private static final int NUM_OF_WRITE_THREADS = 5;
	
	private static final ExecutorService execService = Executors.newFixedThreadPool(NUM_OF_WRITE_THREADS);
	
	private static final Logger logger = LogManager.getLogger(MeasurementFileFactory.class);
		
	static {
		Thread measFeeder = new Thread() {
			
			int currentWriter = -1;
			
			@Override
			public void run() {
				logger.log(Level.INFO, "Feeder Thread Started");
				while (true) {
					try {
						if (queue.isEmpty()) {
							synchronized(LOCK) {
								LOCK.notifyAll();
							}
						}
						Data data = queue.take();
						handleData(data);
					} catch (InterruptedException e) {
						interrupt();
					}
				}
			}
			
			// Round Robin -- needs synchronization at MeasurementFile as multiple threads can write into same file.
			private WriteTask getWriteThread() {
				if (currentWriter == -1 || currentWriter == NUM_OF_WRITE_THREADS-1) {
					currentWriter = 0;
				}  else {
					currentWriter++;
				}
				return writeThreads.get(currentWriter);
			}
			
			//Hash Function
			private WriteTask getWriteThread(String folderName) {
				int index = Math.abs(folderName.hashCode()%NUM_OF_WRITE_THREADS);
				return writeThreads.get(index);
			}
			
			private void handleData(Data data) {
				try {
					//IMeasurementFile measFile;
					WriteTask writeTask = getWriteThread(data.getFolderName());
					switch (data.getFormat()) {
					case 0:
						//measFile = textMeasurementFileMap.get(data.getFolderName());
						//writeTask.setMeasurementFile(measFile);
						writeTask.addData(data);
						//measFile.addData(data.getRows());
						//measFile.saveData();
						break;
					case 1:
						//measFile = binMeasurementFileMap.get(data.getFolderName());
						//measFile.addData(data.getRows());
						//measFile.saveData();
						break;
					case 2:
						//measFile = parquetMeasurementFileMap.get(data.getFolderName());
						//measFile.addData(data.getRows());
						//measFile.saveData();
						break;
					default:
						return;
					}
				} catch (Exception e) {
					logger.log(Level.WARN,"Exception while pushing data :", e );
					
				}
				
			}
		};
		for (int i= 0 ; i < NUM_OF_WRITE_THREADS ; i++ ) {
			WriteTask task = new WriteTask();
			writeThreads.put(i,task);
			logger.log(Level.INFO, "Writer Thread created");
			execService.execute(task);
		}
		measFeeder.start();
	}
	
	
	private MeasurementFileFactory() {

	}


	public static int getMeasurementFileFormat(SourceFile sFile, Logger log) {
		String fileFormatString = sFile.getProperty("outputFormat", "0");
		try {
			return Integer.parseInt(fileFormatString);
		} catch (NumberFormatException ne) {
			log.log(Level.INFO, "Not able to get the outputformat, falling back to detfault - text format");
			return 0;
		}
	}
	

	public static IMeasurementFile getMeasurementFile(int format, String folderName) {
		IMeasurementFile measFile;
		switch (format) {
		case 0:
			measFile = textMeasurementFileMap.get(folderName);
			break;
		case 1:
			measFile = binMeasurementFileMap.get(folderName);
			break;
		case 2:
			measFile = parquetMeasurementFileMap.get(folderName);
			break;
		default:
			return null;
		}
		return measFile;
	}
	
	public static String getFolderName(SourceFile sf, String tagID, Logger log) {
		if (dFCache == null) {
			dFCache = DataFormatCache.getCache();
		}
		DFormat dataFormat;
		dataFormat = dFCache.getDataFormat(tagID);
		
		if (dataFormat == null) {
			log.debug("getFolderName():DataFormat not found for tag: " + tagID );
			return null;
		}
		return dataFormat.getFolderName();
	}

	public static Channel createChannel(SourceFile sFile, String tagID, String techPack, final Logger log, ISink sink) throws Exception {
		boolean checkTagId = Boolean.parseBoolean(sFile.getProperty("checkTagId", FALSE));
		
		if (dFCache == null) {
			dFCache = DataFormatCache.getCache();
		}
		DFormat dataFormat;
		//TODO - implement support for this in dataFormat cache.
		if (!checkTagId) {
			dataFormat = dFCache.getDataFormat(tagID);
		} else {
			dataFormat = dFCache.getFormatByTagID(tagID);
		}
		//dataFormat = dFCache.getDataFormat(tagID);
		if (dataFormat == null) {
			log.debug("DataFormat not found for (tag: " + tagID + ") -> writing nothing.");
			return null;
		}
		int fileFormat = getMeasurementFileFormat(sFile, log);
		createMeasurementFile(fileFormat, sFile, techPack, tagID, dataFormat, log, sink);
		return new Channel(dataFormat.getFolderName(), fileFormat, log);
	}

	public static void createMeasurementFile(int fileFormat, SourceFile sfile, String techPack,String tagId, DFormat dataFormat,
			final Logger log, ISink sink) throws Exception {
		String folderName = dataFormat.getFolderName();
		switch (fileFormat) {
		case 0:
			if (textMeasurementFileMap.get(folderName) == null) {
				textMeasurementFileMap.put(folderName, new TextMeasurementFile(sfile, techPack, tagId, dataFormat, log, sink));
			}
			break;

		case 1:

			if (binMeasurementFileMap.get(folderName) == null) {
				binMeasurementFileMap.put(folderName, new TextMeasurementFile(sfile, techPack, tagId, dataFormat, log, sink));
			}
			break;
		case 2:

			if (parquetMeasurementFileMap.get(folderName) == null) {
				parquetMeasurementFileMap.put(folderName, new TextMeasurementFile(sfile, techPack, tagId, dataFormat, log, sink));
			}
			break;
		default:
			return;
		}

	}

	/**
	 * Should be called at end of each batch
	 */
	public static void closeMeasurementFiles(Logger log) {
		long startTime = System.currentTimeMillis();
		log.info("closing triggered  : queue size before sleeping" + queue.size());
		int retryCount = 0;
		while (!isWriteThreadQueuesEmpty() && retryCount <(50*60*14) ) {
			waitFor(20);
		}
		/*while (!queue.isEmpty()) {
			//Wait for the queue to be consumed.
			synchronized(LOCK) {
				try {
					LOCK.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}*/
		long endTime = System.currentTimeMillis();
		log.info("Time spent in flushing out queues : " + (endTime-startTime));
		List<IMeasurementFile> busyFiles = closeMeasFiles(textMeasurementFileMap);
		busyFiles.addAll(closeMeasFiles(binMeasurementFileMap));
		busyFiles.addAll(closeMeasFiles(parquetMeasurementFileMap));
		retryCount = 0;
		startTime = System.currentTimeMillis();
		while(!busyFiles.isEmpty() && retryCount <= (100*60*14)) {
			//give some time for the measurement files to write // maximum two minutes
			retryCount ++;
			waitFor(10);
			busyFiles = closeMeasFiles(textMeasurementFileMap);
		}
		busyFiles.forEach(IMeasurementFile::close);
		endTime = System.currentTimeMillis();
		log.info("Time spent waiting for measurement file to complete pushing : " + (endTime-startTime));
		textMeasurementFileMap.clear();
		binMeasurementFileMap.clear();
		parquetMeasurementFileMap.clear();
	}
	
	
	private static boolean isWriteThreadQueuesEmpty() {
		boolean result = true;
		for (WriteTask writeThread : writeThreads.values()) {
			 result = result && writeThread.isQueueEmpty() ;
		}
		return result;
	}
	
	private static void waitFor(long millis) {
		try {
			
			Thread.sleep(millis);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	private static List<IMeasurementFile> closeMeasFiles(Map<String, IMeasurementFile> measFiles) {
		//System.out.println("closing triggered  : closeMeasFiles");
		List<IMeasurementFile> busyFiles = new ArrayList<>();
		measFiles.forEach( (key, measFile) -> {
			if (measFile.getStatus() != MeasurementFileStatus.WRITING_INPROGRESS) {
				measFile.close();
			} else {
				busyFiles.add(measFile);
			}
		});
		return busyFiles;
	}

	public static class Data {
		String folderName;
		int format;
		Map<String, String> rows;

		Data(String folderName, int format, Map<String, String> rows) {
			this.folderName = folderName;
			this.format = format;
			this.rows = rows;
		}

		String getFolderName() {
			return folderName;
		}

		int getFormat() {
			return format;
		}

		Map<String, String> getRows() {
			return rows;
		}
	}
	
	public static class Channel{
		private String folderName;
		private int format;
		private Logger log;
				
		private Channel(String folderName, int format, Logger log) {
			this.folderName = folderName;
			this.format = format;
			this.log = log;
		}
		
		public void pushData(Map<String, String> rows) {
			Data d = new Data(folderName, format, rows);
			try {
				//log.log(Level.INFO, "data pushed for " + folderName);
				queue.put(d);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				log.log(Level.WARN, "Thread got interrupted");
			}
		}
		
		@SuppressWarnings("unused")
		private void pushData(String key, String value) {
			Map<String, String> rows = new HashMap<>();
			rows.put(key, value);
			pushData(rows);
		}
				
	}

}
