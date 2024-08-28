package com.distocraft.dc5000.etl.parser.xmltransformer;

public class ConfigException extends Exception {
  
  private ConfigException() {
    
  }
  
  public ConfigException(String message) {
    super(message);
  }
  
  public ConfigException(String message, Exception cause) {
    super(message,cause);
  }

}
