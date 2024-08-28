package com.ericsson.eniq.parser.binaryformatter;

import java.nio.ByteOrder;

public class UnsignedintFormatter extends BinFormatter {

  public byte[] doFormat(final String value, final int... sizeDetails) throws NumberFormatException {

    if (value == null || value.length() == 0) {
      final byte[] ret = new byte[5];

      for (int i = 0; i < 4; i++) {
        ret[i] = (byte) 0;
      }
      ret[4] = (byte) 1;

      return ret;

    } else {
      final long i = Long.parseLong(value);

      return doFormat(i);
    }
  }

  byte[] doFormat(final long i) {
    final byte[] ret = new byte[5];

    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
      ret[0] = (byte) (i & 0xFF);
      ret[1] = (byte) ((i >> 8) & 0xFF);
      ret[2] = (byte) ((i >> 16) & 0xFF);
      ret[3] = (byte) ((i >> 24) & 0xFF);
    } else {
      ret[0] = (byte) ((i >> 24) & 0xFF);
      ret[1] = (byte) ((i >> 16) & 0xFF);
      ret[2] = (byte) ((i >> 8) & 0xFF);
      ret[3] = (byte) (i & 0xFF);
    }

    ret[4] = (byte) 0; // null byte

    return ret;
  }

}
