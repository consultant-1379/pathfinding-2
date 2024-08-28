package com.distocraft.dc5000.etl.binaryformatter;

import java.nio.ByteOrder;


public class BigintFormatter extends BinFormatter {
    
  @Override
  /**
   * bigint spesific format
   */
  public byte[] doFormat(final String value, final int ... sizeDetails) throws NumberFormatException {
    if(value == null || value.length() == 0) {
      
      final byte[] ret = new byte[9];
      
      for (int i = 0; i < 8; i++) {
        ret[i] = (byte)0;
      }
      ret[8] = (byte)1;
      
      return ret;
      
    } else {

      final long i = Long.parseLong(value);
      
      return doFormat(i);      
    }
  }
  
  byte[] doFormat(final long i) {
    
    final byte[] ret = new byte[9];
    
    if(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      ret[0] = (byte) (i & 0xFF);
      ret[1] = (byte) ((i >> 8) & 0xFF);
      ret[2] = (byte) ((i >> 16) & 0xFF);
      ret[3] = (byte) ((i >> 24) & 0xFF);
      ret[4] = (byte) ((i >> 32) & 0xFF); 
      ret[5] = (byte) ((i >> 40) & 0xFF);
      ret[6] = (byte) ((i >> 48) & 0xFF);
      ret[7] = (byte) ((i >> 56) & 0xFF);
    } else {
      ret[0] = (byte) ((i >> 56) & 0xFF);
      ret[1] = (byte) ((i >> 48) & 0xFF);
      ret[2] = (byte) ((i >> 40) & 0xFF);
      ret[3] = (byte) ((i >> 32) & 0xFF);
      ret[4] = (byte) ((i >> 24) & 0xFF);
      ret[5] = (byte) ((i >> 16) & 0xFF);
      ret[6] = (byte) ((i >> 8) & 0xFF);
      ret[7] = (byte) (i & 0xFF);
    }
    
    ret[8] = (byte)0; // null byte
    
    return ret;
    
  }
  
}
