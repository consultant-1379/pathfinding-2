package com.distocraft.dc5000.etl.binaryformatter;


public class BinaryFormatter extends BinFormatter {

  @Override
  /**
   * binaryspesific format
   */
  public byte[] doFormat(final String value, final int ... sizeDetails) throws Exception {
    final byte[] ret = new byte[sizeDetails[0] + 1];
    if(value != null){
      final byte[] raw = value.getBytes();
      System.arraycopy(raw, 0, ret, 0, raw.length);
    }
    return ret;
  }
}
