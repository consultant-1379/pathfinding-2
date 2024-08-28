package com.ericsson.eniq.parser.binaryformatter;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is superclass of all the binary formatters. Purpose of this class is to
 * provide interface to create datatype spesific instance of subclass of this
 * class. Sublasses are named as [<datatype>"Formatter"]
 * 
 * Subclasses represent each datatype in sybase iq database: bit, tinyint,
 * smallint, int/unsigned int, bigint/insigned bigint, float, double,
 * char/varchar, binary/varbinary
 * 
 * @author etogust
 */
public abstract class BinFormatter {

  /**
   * Formatters
   */
  @SuppressWarnings({"PMD.SuspiciousConstantFieldName"})
  private static HashMap<String, BinFormatter> CLASSES = null;
  //Fair lock option set to false to verify the performance issue.  
  private static ReadWriteLock binFormatterCacheRWLock = new ReentrantReadWriteLock();
  

  /**
   * This method takes string value as parameter and returns intarray
   * representing the value as bytes which can be used in binary load files for
   * sybase IQ database.
   * 
   * @param value
   +*          Value in database as String
   * @param sizeDetails
   *          Int value telling the possible size and possible precision of the
   *          column
   * @return The string value as a byte array, last index in null byte
   * @throws Exception
   *           Exception is thrown if given String value cannot be parsed
   */
  public abstract byte[] doFormat(final String value, final int... sizeDetails) throws Exception;

  /**
   * Gets the static CLASSES HashMap, which stores information about which class
   * is used to which dadtatype
   * 
   * @return Map containing the datatype and formatter to use to convert
   */
  public static  HashMap<String, BinFormatter> getClasses() {
	  binFormatterCacheRWLock.readLock().lock();
	  try {
			return CLASSES;
		} finally {
			binFormatterCacheRWLock.readLock().unlock();
		}
	   }

   public static void createClasses() {
	   
	   binFormatterCacheRWLock.writeLock().lock();
	   try {
		   CLASSES = new HashMap<String, BinFormatter>();

    	   CLASSES.put("bit", new BitFormatter());
    	   CLASSES.put("tinyint", new TinyintFormatter());
    	   CLASSES.put("smallint", new SmallintFormatter());
    	   CLASSES.put("int", new IntFormatter());
    	   CLASSES.put("integer", new IntFormatter());
    	   CLASSES.put("unsigned int", new UnsignedintFormatter());
    	   CLASSES.put("bigint", new BigintFormatter());
    	   CLASSES.put("unsigned bigint", new UnsignedbigintFormatter());
    	   CLASSES.put("float", new FloatFormatter());
    	      // CLASSES.put("double", new DoubleFormatter());
    	   CLASSES.put("char", new CharFormatter());
    	   CLASSES.put("varchar", new VarcharFormatter());
    	   CLASSES.put("binary", new BinaryFormatter());
    	   CLASSES.put("varbinary", new VarbinaryFormatter());
    	   CLASSES.put("date", new DateFormatter());
    	   CLASSES.put("time", new TimeFormatter());
    	   CLASSES.put("datetime", new DatetimeFormatter());
    	   CLASSES.put("numeric", new NumDecFormatter());
    	   CLASSES.put("decimal", new NumDecFormatter());		   
	   } finally {
		   binFormatterCacheRWLock.writeLock().unlock();
	   }
	   }


  static byte[] intToByteArray(final int value, final byte[] b) {
    for (int i = 0; i < 4; i++) {
      final int offset = (b.length - 1 - i) * 8;
      b[i] = (byte) ((value >>> offset) & 0xFF);
    }
    return b;
  }
  
  static int int_swap (final int value) {
    final int b1 = (value) & 0xff;
    final int b2 = (value >>  8) & 0xff;
    final int b3 = (value >> 16) & 0xff;
    final int b4 = (value >> 24) & 0xff;

    return b1 << 24 | b2 << 16 | b3 << 8 | b4;
  }

  byte[] doFormat(final int i) {
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
