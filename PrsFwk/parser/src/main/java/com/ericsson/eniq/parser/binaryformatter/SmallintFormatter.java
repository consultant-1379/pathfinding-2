package com.ericsson.eniq.parser.binaryformatter;

import java.nio.ByteOrder;

public class SmallintFormatter extends BinFormatter {

  @Override
  /**
   * smallint spesific format
   */
  public byte[] doFormat(final String value, final int... sizeDetails) throws NumberFormatException {

    final byte[] ret = new byte[3];
    
    if (value == null || value.length() == 0) {
      ret[0] = 0;
      ret[1] = 0;
      ret[2] = 1;
    } else {
      
      final int i = Integer.parseInt(value);

      if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
        ret[0] = (byte) (i & 0xFF);
        ret[1] = (byte) ((i >> 8) & 0xFF);
      } else {
        ret[0] = (byte) ((i >> 8) & 0xFF);
        ret[1] = (byte) (i & 0xFF);
      }

      ret[2] = (byte) 0; // null byte
      
    }
    
    return ret;
    
  }
}
