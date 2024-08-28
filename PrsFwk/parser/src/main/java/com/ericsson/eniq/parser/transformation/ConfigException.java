package com.ericsson.eniq.parser.transformation;

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
