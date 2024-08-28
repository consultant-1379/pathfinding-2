package com.ericsson.eniq.parser.binaryformatter;

import java.math.BigInteger;
import java.nio.ByteOrder;

public class UnsignedbigintFormatter extends BinFormatter {

  @Override
  /*
   * bigint spesific format
   */
  public byte[] doFormat(final String value, final int... sizeDetails) throws Exception {

    if (value == null || value.length() == 0) {
      final byte[] ret = new byte[9];
      for (int i = 0; i < 8; i++) {
        ret[i] = 0;
      }
      ret[8] = 1;

      return ret;
    } else {
      return doFormat(new BigInteger(value));
    }

  }

  byte[] doFormat(final BigInteger value) throws Exception {
    
    final byte[] byteArr = value.toByteArray();
    final byte[] ret = new byte[9];    

    for(int i = 0 ; i < 9 ; i++) {
      ret[i] = 0;
    }
    
    if(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      int pos = byteArr.length - 1;

      for(int i = 0 ; i <= 7 && pos >=0 ; i++) {
        ret[i] = byteArr[pos--];
      }
    } else {
      int pos = byteArr.length - 1;
      
      for(int i = 7 ; i >= 0 && pos >= 0; i--) {
        ret[i] = byteArr[pos--];
      }
    }
    
    return ret;
    
  }
}
