package com.ericsson.eniq.parser.binaryformatter;

public class VarcharFormatter extends BinFormatter {

  /**
   * varchar specific format
   */
  public byte[] doFormat(final String value, final int ... sizeDetails) {
    
    final byte[] ret = new byte[sizeDetails[0] + 1];
    
    for (int i = 0; i < ret.length - 1; i++) {
      ret[i] = 32;
    }
    
    ret[ret.length - 1] = 0; //Setting the NULL-byte to zero - ensures returned byte array is non-NULL. 
    
    if(value == null || value.length() == 0) {
        //If value is empty string or null, return blank byte array (null byte array should not be returned for varchar format)
    	return ret; 
    }
    
    final byte[] arr = value.getBytes();
    
    int i = 0;
    for(i = 0 ; i < arr.length && i < ret.length-1 ; i++) {
      ret[i] = arr[i];
    }
       
    return ret;
  }
}
