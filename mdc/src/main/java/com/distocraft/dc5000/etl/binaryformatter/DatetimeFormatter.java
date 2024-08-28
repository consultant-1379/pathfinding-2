package com.distocraft.dc5000.etl.binaryformatter;

import java.math.BigInteger;

public class DatetimeFormatter extends BinFormatter {

  private UnsignedbigintFormatter ubfor = new UnsignedbigintFormatter();
  private TimeFormatter tform = new TimeFormatter();
  private DateFormatter dform = new DateFormatter();
  
  @Override
  /*
   * date spesific format
   */
  public byte[] doFormat(final String value, final int... size) throws Exception {
    // format is YYYY-MM-DD HH:mm:ss.S parse values

    if (value == null || value.length() == 0) {
      final byte[] ret = new byte[9];
      for (int i = 0; i < 8; i++) {
        ret[i] = 0;
      }
      ret[8] = 1;
      
      return ret;

    } else {

      final String[] parts = value.split(" ");
      
      if(parts.length != 2) {
        throw new Exception("Unknown datetime format: " + value);
      }
      
      final BigInteger binaryDateValue = BigInteger.valueOf(dform.getBinaryValue(parts[0]));
      final BigInteger binaryTimeValue = tform.getBinaryValue(parts[1]);
      
      final BigInteger hos_date_scale = BigInteger.valueOf(86400000000L);
      
      final BigInteger binarydatetimevalue = binaryDateValue.multiply(hos_date_scale).add(binaryTimeValue);
     
     return ubfor.doFormat(binarydatetimevalue); 
      
    }
  }
  
}
