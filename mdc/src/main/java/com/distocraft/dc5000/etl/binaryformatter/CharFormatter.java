package com.distocraft.dc5000.etl.binaryformatter;

public class CharFormatter extends VarcharFormatter {

  @Override
  /**
   * char spesific format
   */
  public byte[] doFormat(final String value, final int... sizeDetails) {
    final byte[] ret = new byte[sizeDetails[0] + 1];
    for (int i = 0; i < ret.length - 1; i++) {
      ret[i] = 32;
    }
    if(value == null) {
      ret[ret.length - 1] = 1;
      return ret;
    } else if(value.length() == 0) {
      ret[ret.length - 1] = 0;
      return ret;
    }
    final char[] arr = value.toCharArray();
    for(int i = 0 ; i < arr.length && i < ret.length-1 ; i++) {
      ret[i] = (byte)arr[i];
    }
    ret[ret.length - 1] = 0;
    return ret;
  }
}
