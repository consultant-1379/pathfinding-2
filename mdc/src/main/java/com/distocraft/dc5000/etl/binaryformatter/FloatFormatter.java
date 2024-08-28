package com.distocraft.dc5000.etl.binaryformatter;

public class FloatFormatter extends BinFormatter {

  @Override
  /**
   * float spesific format
   */
  public byte[] doFormat(final String value, final int... sizeDetails) throws NumberFormatException {
    float floatValue = 0;
    if(value != null && !"".equals(value)){
      floatValue = Float.valueOf(value);
    }
    final int rawInt = Float.floatToIntBits(floatValue);
    return doFormat(rawInt);
  }
}
