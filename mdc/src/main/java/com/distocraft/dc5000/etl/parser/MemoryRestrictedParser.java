package com.distocraft.dc5000.etl.parser;


/**
 * Created on 6th of May 2009
 * Interface for technology specific parser implementations having problems with memory.
 * @author unknown
 */
public interface MemoryRestrictedParser extends Parser {
  
  /**
   * Parses the source file specified by SourceFile object.
   * @param sf SourceFile for parsing.
   * @throws Exception thrown in case of failure.
   */
  int memoryConsumptionMB();
  void setMemoryConsumptionMB(int memoryConsumptionMB);
}
