package com.distocraft.dc5000.etl.binaryformatter;

import java.util.Calendar;

public class DateFormatter extends BinFormatter {

  private final UnsignedintFormatter uiform = new UnsignedintFormatter();
  
  @Override
  /**
   * date spesific format String form of date is "YYYY-MM-DD"
   */
  public byte[] doFormat(final String value, final int... sizeDetails) throws Exception {

    if (value == null || value.length() == 0) {
      final byte[] ret = new byte[5];
      for(int i = 0 ; i < 4 ; i++) {
        ret[i] = 0;
      }
      ret[4] = 1;
      
      return ret;
      
    } else {
    
      return uiform.doFormat(getBinaryValue(value));
      
    }
    
  }
  
  long getBinaryValue(final String value) throws Exception {
    int YYYY;
    final int MM, DD, jDay;
    
    try {
      final Calendar c = Calendar.getInstance();

      final String[] parts = value.split("-");
      YYYY = Integer.parseInt(parts[0]);
      MM = Integer.parseInt(parts[1]);
      DD = Integer.parseInt(parts[2]);

      c.set(YYYY, MM - 1, DD);
      jDay = c.get(Calendar.DAY_OF_YEAR);

    } catch (Exception ex) {
      throw new Exception("Unknown date format: " + value);
    }

    // Do the trick
    YYYY--;
    return (YYYY * 365) + (YYYY / 4) - (YYYY / 100) + (YYYY / 400) + 366 + jDay - 1;
  }
  
}
