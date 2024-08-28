package com.ericsson.eniq.parser.binaryformatter;

import java.math.BigInteger;

public class TimeFormatter extends BinFormatter {

  public UnsignedbigintFormatter ubform = new UnsignedbigintFormatter();

  @Override
  /*
   * date spesific format
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

      return ubform.doFormat(getBinaryValue(value));

    }
  }

  BigInteger getBinaryValue(final String value) throws Exception {
    // format is HH:mm:ss, parse values

    final long HH;
    final long mm;
    final long ss;

    try {
      final String[] parts = value.split(":");
      HH = Long.parseLong(parts[0]);
      mm = Long.parseLong(parts[1]);
      ss = Long.parseLong(parts[2]);
    } catch (Exception ex) {
      throw new Exception("Unknown time format: " + value);
    }

    final long secs = ss + mm * 60 + HH * 3600;

    final BigInteger bsecs = BigInteger.valueOf(secs);
    final BigInteger mses = BigInteger.valueOf(1000000);

    return bsecs.multiply(mses);

  }

}
