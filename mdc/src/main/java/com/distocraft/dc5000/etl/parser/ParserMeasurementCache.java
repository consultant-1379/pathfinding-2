package com.distocraft.dc5000.etl.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.cache.DFormat;
import com.distocraft.dc5000.repository.cache.DataFormatCache;

/**
 * This Class is used to cache parsed data according to given keys and up to
 * ginen max number of rows.
 * 
 * @author etogust
 * 
 */
public class ParserMeasurementCache {

  /**
   * Needed for measurementFile and finding the foldername
   */
  private static DataFormatCache dfCache = null;

  // logger for logging
  private Logger log;

  // Needed for tarnsformations
  private DFormat dataformat;

  // max size of cache in rows. (Rows after joining)
  private int maxSize = 0;

  /**
   * cache conatining the rows as value. Key is the generated key according to
   * the keyConf
   */
  private Map<String, Map<String, String>> joinedCache = null;

  /**
   * cache conatining the rows as value. Key is the generated key according to
   * the keyConf
   */
  private ArrayList<Map<String, String>> cache = null;

  /**
   * CurrentROw which is being handled at the moment
   */
  private Map<String, String> currentrow = null;

  /**
   * TP name (Needed for measurementFile)
   */
  private String TP;

  /**
   * Set type (Needed for measurementFile)
   */
  private String setType;

  /**
   * Set name (Needed for measurementFile)
   */
  private String setName;

  /**
   * WorkerName (Needed for measurementFile)
   */
  private String workerName;

  /**
   * Sourcefile (Needed for measurementFile)
   */
  private SourceFile sourceFile;

  /**
   * Constructor
   * 
   * @param log
   * @param maxSize
   * @param keyConf
   */
  public ParserMeasurementCache(Logger log, int maxSize) {
    this.log = log;
    this.maxSize = maxSize;
    this.cache = new ArrayList<Map<String, String>>();
    this.joinedCache = new HashMap<String, Map<String, String>>();

  }

  /**
   * Default Constructor
   */
  public ParserMeasurementCache() {
    this(Logger.getLogger("parser.ParserMeasurementCache"), 0);
  }

  /**
   * Saves cached data to outputfile (loadfile)
   * 
   * @throws Exception
   */
  public void saveData()  {
    HashMap<String, ArrayList<Map<String, String>>> vTagMapJoined = new HashMap<String, ArrayList<Map<String, String>>>();
    HashMap<String, ArrayList<Map<String, String>>> vTagMapNormal = new HashMap<String, ArrayList<Map<String, String>>>();

    // Handle joined rows
    Set<String> keySet = joinedCache.keySet();
    for (String key : keySet) {
      // Order the rows per vendorTag to minimize Opening/closing outputfiles
      Map<String, String> row = joinedCache.get(key);
      String vendorTag = row.get("vendorTag");
      ArrayList<Map<String, String>> rowList = vTagMapJoined.get(vendorTag);

      if (rowList == null) {
        rowList = new ArrayList<Map<String, String>>();
        vTagMapJoined.put(vendorTag, rowList);
      }
      rowList.add(row);
    }

    // Handle normal rows
    for (Map<String, String> row : cache) {
      String vendorTag = row.get("vendorTag");
      ArrayList<Map<String, String>> rowList = vTagMapNormal.get(vendorTag);

      if (rowList == null) {
        rowList = new ArrayList<Map<String, String>>();
        vTagMapNormal.put(vendorTag, rowList);
      }
      rowList.add(row);
    }

    doSaveMap(vTagMapJoined, true);
    doSaveMap(vTagMapNormal, false);

    // Last step is to clear cache
    cache.clear();
    joinedCache.clear();
  }

  /**
   * 
   * @param tagMap
   * @throws Exception
   */
  private void doSaveMap(HashMap<String, ArrayList<Map<String, String>>> tagMap, boolean transformed)  {
    if (tagMap == null || tagMap.size() == 0)
      return;

    // Now go through each set of rows and write to file
    Set<String> vendorTags = tagMap.keySet();
    try{
    for (String vendorTag : vendorTags) {

      ArrayList<Map<String, String>> rowList = tagMap.get(vendorTag);
      MeasurementFile measFile = Main.createMeasurementFile(sourceFile, vendorTag, TP, setType, setName, workerName,
          log);

      for (Map<String, String> row : rowList) {
        measFile.setTransformed(transformed);
        measFile.setData(row);
        measFile.saveData();
      }
      measFile.close();
    }} catch (Exception e) {
      log.log(Level.WARNING, "Parser cache failed to exception while writing outputfiles", e);
    }
  }

  /**
   * Take one meas,key pair as part of one row
   * 
   * @param key
   * @param value
   */
  public void put(String key, String value) {
    if (currentrow == null)
      currentrow = new HashMap<String, String>();

    currentrow.put(key, value);
  }

  /**
   * End of row is reached. Do the joining and save to file if needed.
   * 
   * @param techPack
   * @param vendorTag
   * @throws Exception
   */
  public void endOfRow(String vendorTag, String techPack)throws Exception {
    if (currentrow == null)
      return;
    
    if (dfCache == null) {
      dfCache = DataFormatCache.getCache();
    }

    String interfaceName = sourceFile.getProperty("interfaceName");
    dataformat = dfCache.getFormatWithTagID(interfaceName, vendorTag);

    if (dataformat == null) {
      log.fine("DataFormat not found for (tag: " + vendorTag + ") -> writing nothing.");
      return;
    }
    String keyConf = this.sourceFile.getProperty(dataformat.getFolderName(), null);
    final Transformer t = TransformerCache.getCache().getTransformer(dataformat.getTransformerID()); // TransformerCache

    // Note that we only transform for the joined types
    if (t != null && keyConf != null)
      t.transform(currentrow, log);

    String cacheKey = genKey(currentrow, keyConf);

    // Join or add new row. In case of null key, don't do any joining
    if (cacheKey != null) {
      // Take the "prerow" or create new one
      Map<String, String> cachedRow = joinedCache.get(cacheKey);
      if (cachedRow == null) {
        cachedRow = new HashMap<String, String>();
        joinedCache.put(cacheKey, cachedRow);
      }
      // Go through currentrow and fill new values to joined row
      Set<String> columns = currentrow.keySet();
      for (String column : columns) {
        String value = cachedRow.get(column);
        if (value == null || value.equalsIgnoreCase(""))
          cachedRow.put(column, currentrow.get(column));
      }
    } else {
      cache.add(currentrow);
    }

    currentrow = null;
    if (cache.size() + joinedCache.size() == maxSize)
      saveData();
  }

  /**
   * GenerateKey for this row
   * 
   * @param row
   * @param keyConf
   */
  private String genKey(Map<String, String> row, String keyConf) {
    if (keyConf == null || keyConf.length() == 0)
      return null;

    String[] keyParts = keyConf.split("\\.");

    StringBuilder key = new StringBuilder();

    key.append(dataformat.getFolderName());
    for (String keyPart : keyParts) {
      key.append(".");
      key.append(row.get(keyPart));
    }
    return key.toString();
  }

  /**
   * Setters of private members
   * 
   */
  public void setTP(String techPack) {
    this.TP = techPack;
  }

  public void setSetType(String setType) {
    this.setType = setType;
  }

  public void setSetName(String setName) {
    this.setName = setName;
  }

  public void setWorkerName(String workerName) {
    this.workerName = workerName;
  }

  public void setSourceFile(SourceFile sf) {
    this.sourceFile = sf;
  }
}
