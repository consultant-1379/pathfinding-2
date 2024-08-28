package com.distocraft.dc5000.etl.binaryformatter;

import java.nio.ByteOrder;
import java.util.logging.Logger;

public strictfp class NumDecFormatter extends BinFormatter {

  private final SmallintFormatter smform = new SmallintFormatter();

  private final IntFormatter inform = new IntFormatter();

  private final BigintFormatter biform = new BigintFormatter();
  
 private Logger log = Logger.getLogger(NumDecFormatter.class.getName());
     
  @Override
  public byte[] doFormat(final String value, final int... sizeDetails) throws Exception {

    if (sizeDetails.length < 2) {
      throw new Exception("Numeric data item does not have information about size and scale");
    }

    final int size = sizeDetails[0];
    final int precision = sizeDetails[1];

    String[] valArr = null;
    if (value != null){
    	valArr = value.split("[\\.,]");
    }

    String parsedValue = null;

    if (value != null && value.length() > 0) {

      final StringBuilder number;
      if (valArr.length > 0 && valArr[0] != null) {
        number = new StringBuilder(valArr[0]);
      } else {
        number = new StringBuilder();
      }

      final StringBuilder fraction;
      if (valArr.length > 1 && valArr[1] != null) {
        fraction = new StringBuilder(valArr[1]);
      } else {
        fraction = new StringBuilder();
      }

      while (fraction.length() < precision) {
        fraction.append("0");
      }
      while (fraction.length() > precision) {
	        fraction.deleteCharAt(fraction.length()-1);
	  }

      parsedValue = "" + number + fraction;

    }

    if (size <= 4) {
      return smform.doFormat(parsedValue, sizeDetails);
    } else if (size >= 5 && size <= 9) {
      return inform.doFormat(parsedValue, sizeDetails);
    } else if (size >= 10 && size <= 18) {
      return biform.doFormat(parsedValue, sizeDetails);
    } else {
      return specialFormat(parsedValue, size, precision);
    }

  }
 
  private byte[] specialFormat(final String value, final int size, final int precision) {
	 
	/**
	 * As per the SYBASE documentation, when the precision exceeds 18, then the below formula should
	 * be used to calculate storage requirement in bytes.
	 * Formula : 4 + 2 * (int(((prec - scale) + 3) / 4) +  int((scale + 3) / 4) + 1)
	 * where 'prec' is total digits, 'scale' is digits after decimal
	 */
		 
	int bytesRequired = 4 + 2 * ((int)(((size - precision) + 3) / 4) + (int)((precision + 3) / 4) + 1);
    final byte[] digits = new byte[bytesRequired+1];
    for (int i = 0; i < digits.length; i++) {
      digits[i] = 0;
    }
    
    if (null == value) {
      digits[0] = 1;
      digits[bytesRequired] = (byte)1; //Set the null byte to indicate null value.
      return digits;  //return null
    }
    digits[bytesRequired] = (byte)0;  //Set the null byte to indicate non-null value.
    
    final byte sign = value.charAt(0) == '-' ? (byte)0 : (byte)1; // sign 1 for +, 0 for -

    final int intLength = value.length() - precision;
    String integral;
    String fraction;
    if(value.length() > intLength){
      integral = value.substring(0, intLength);
      fraction = value.substring(intLength);
    } else {
      integral = value;
      fraction = "";
    }


    integral = integral.replaceAll("^[-0\\+]*", "");
    fraction = fraction.replaceAll("0*$", "");

    while (fraction.length() % 4 != 0){
      fraction += "0";
    }

    final int intLeng = (int) Math.ceil(integral.length() / 4.0);
    final int fracLeng = (int) Math.ceil(fraction.length() / 4.0);
    final int[] digitArr = new int[intLeng + fracLeng];
    final int excess = 80 - fracLeng;

    String str = fraction;
    int start = str.length() - 4;
    int len = 4;
    for (int i = 0; i < digitArr.length; i++) {

      if (start < 0) {
        len += start;
        start = 0;

        if (len <= 0) {
          str = integral;
          start = str.length() >= 4 ? str.length() - 4 : 0;
          len = str.length() >= 4 ? 4 : str.length();
        }
      }

      digitArr[i] = Integer.parseInt(str.substring(start, start + len));
      start -= 4;
    }
    
    digits[0] = sign;
    digits[1] = 0;
    digits[2] = 1;
    digits[3] = 0;

    if ((fraction + integral).equals("")) {
      return digits;
    }
    //digit[0] - should contain sign
    //digit[1] - should contain number of digits
    //digit[2] - should contain the exponent
    //digit[3] - should contain the Erracc - always 0
    //Then the following values should be byte representation of fraction 
    // followed by byte representation of integral
    //Left over bytes should be zero.
    int byteHelp = Integer.parseInt("11111111", 2);
    digits[2] = (byte)(byteHelp & excess);
    digits[3] = 0;
    digits[1] = (byte)digitArr.length;

    byteHelp = Integer.parseInt("11111111", 2);
    int digitIndex = 0;
    for (int i = 4; i < digits.length - 1; i += 2) {

      if (digitArr.length > digitIndex) {
        int digit = digitArr[digitIndex];
        digits[i] = (byte)(digit & byteHelp);
        digit = digit >> 8;
        digits[i+1] = (byte)(digit & byteHelp);
        digitIndex++;
      }

    }
    
    if(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
    	//Temporary fix for "Little Endian Error", It has to be revisited
    	log.finest("LITTLE ENDIAN BYTE ORDER");
    } else {
    	log.finest("BIG ENDIAN BYTE ORDER");
    	//Taking the little endian version of data (currently in digits[]) and reversing the order.
    	final byte[] bigEndDigits = new byte[digits.length];
    	final int storageBytes = digits.length - 1;
    	
    	for (int i=0; i<storageBytes; i++){
          bigEndDigits[i] = digits[(storageBytes-1) - i];
    	}
    	
    	bigEndDigits[bytesRequired] = digits[bytesRequired];
    			
        return bigEndDigits;
    	
    }
    
    return digits;

  }
}
