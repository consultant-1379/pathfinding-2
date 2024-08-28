package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public interface Transformation {

  /**
   * This method is used to configure this transformation. All new classes must implement this
   * configuration method. All properties key values are in uppercase.
   * 
   * @param srcKey
   *          The source key to the data map
   * @param tgtKey
   *          The target key to the data map. Target key may be null if the target key(s) is
   *          hardcoded in transformation.
   * @param props
   *          The configuration of transformer
   * @param logNamePFX
   *          Prefix for loggername the transformed uses
   * @exception ConfigException
   *              is thrown if transformed fails to initialize
   */
  void configure(String name, String srcKey, String tgtKey, Properties props, Logger clog) throws ConfigException;

  /**
   * Performs actual transformation.
   * 
   * @param data
   *          Map of data value
   * @exception is
   *              thrown if transformation fails exceptionally
   */
  void transform(Map data, Logger clog) throws Exception;

  String getSource() throws Exception; 
  String getTarget() throws Exception;
  String getName() throws Exception;
  
}
