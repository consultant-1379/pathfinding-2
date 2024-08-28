package com.distocraft.dc5000.etl.binaryformatter;

public class IntFormatter extends BinFormatter {

  @Override
  /*
   * int spesific format
   */
  public byte[] doFormat(final String value, final int... sizeDetails) throws NumberFormatException {

    if (value == null || value.length() == 0) {
      final byte[] ret = new byte[5];

      for (int i = 0; i < 4; i++) {
        ret[i] = (byte) 0;
      }
      ret[4] = (byte) 1;

      return ret;

    } else {
      final int i = Integer.parseInt(value);

      return doFormat(i);
    }
  }

}
