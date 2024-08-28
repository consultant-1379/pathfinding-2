package com.distocraft.dc5000.etl.parser;

import java.util.Map;

import com.distocraft.dc5000.repository.cache.DFormat;

/**
 * MeasurementFile of parser.<br>
 * <br>
 * $id$ <br>
 * <br>
 * Copyright Distocraft 2005 <br>
 * 
 * @author lemminkainen
 */
public interface MeasurementFile {

  /**
   * Sets debugger for this measurementFile
   */
  void setDebugger(ParserDebugger debugger);

  /**
   * Determines wheather metadata was found for created measurement file.
   * 
   * @return true if metadata was found false otherwise
   */
  boolean metadataFound();

  /**
   * Adds one name value pair to MeasurementFile
   * 
   * @param name  attribute name
   * @param value attribute value
   */
  void addData(String name, String value);

  /**
   * Adds a Map of data to MeasurementFile
   * 
   * @param map
   *          Map of data
   */
  void addData(Map map);

  /**
   * Sets the data to Measurement
   */
  void setData(Map map);

  /**
   * Commits one line of data to physical measurement file and resets the
   * internal storage
   */
  boolean hasData();
  void saveData() throws Exception;

  /**
   * Closes the measurement file
   */
  void close() throws Exception;

  /**
   * Determines weather this Measurement file is open
   * 
   * @return <code>true</code> if file is open. <code>false</code> if
   *         measurement file is already closed or metadata was not found for
   *         this file.
   */
  boolean isOpen();

  /**
   * Return amount of rows generated to this MeasurementFile
   */
  int getRowCount();

  /**
   * Return amount of not null counters within this MeasurementFile
   */
  long getCounterVolume();
  
  /**
   * Return datetime_id read from the first row of this MeasurementFile
   */
  String getDatetimeID();
  
  /**
   * Return typename of this MeasurementFile
   */
  String getTypename();
  
  /**
   * Return dataformat of this MeasurementFile
   */
  DFormat getDataformat();
  
  void setTransformed (boolean isTransformed);

  String getTagID();
  /**
   * Return timelevel of this MeasurementFile
   */
  String getTimeLevel();

}
