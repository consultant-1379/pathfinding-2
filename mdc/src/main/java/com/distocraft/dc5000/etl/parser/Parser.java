package com.distocraft.dc5000.etl.parser;


/**
 * Created on Jan 18, 2005
 * Interface for technology specific parser implementations.
 * @author lemminkainen
 */
public interface Parser extends Runnable{
  
  /**
   * Parses the source file specified by SourceFile object.
   * @param sf SourceFile for parsing.
   * @throws Exception thrown in case of failure.
   */
  void parse(SourceFile sf, String techPack, String setType, String setName) throws Exception;
  void init(Main main, String techPack, String setType, String setName, String workerName);
  int status();
  
}
