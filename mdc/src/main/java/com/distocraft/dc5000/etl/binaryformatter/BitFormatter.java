package com.distocraft.dc5000.etl.binaryformatter;


public class BitFormatter extends BinFormatter {

  @Override
  /**
   * Bit spesific format
   */
  public byte[] doFormat(final String value, final int ... sizeDetails) throws NumberFormatException {
    // bit is either 0 | 1
    int intValue = 0;
    if(value != null && !"".equals(value)){
      intValue = Integer.parseInt(value);
    }
    return new byte[]{ intValue<=0 ? (byte)0 : (byte)1, (byte)0};
  }
}
