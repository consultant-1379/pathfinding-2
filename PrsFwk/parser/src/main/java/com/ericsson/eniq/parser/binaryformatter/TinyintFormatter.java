package com.ericsson.eniq.parser.binaryformatter;

public class TinyintFormatter extends BinFormatter {

  @Override
  /**
   * tinyint spesific format
   */
  public byte[] doFormat(final String value, final int ... sizeDetails) throws NumberFormatException {
    final byte[] ret = new byte[2];
    if (value == null || value.length() == 0) {
      ret[0] = 0;
      ret[1] = 1;
    } else {
      final int ival = Integer.parseInt(value);
      ret[0] = (byte)ival;
      ret[1] = 0;
    }
    return ret;
  }

}
