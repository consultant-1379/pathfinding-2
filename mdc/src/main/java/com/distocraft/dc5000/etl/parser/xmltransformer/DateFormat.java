package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This transformer class transforms a date value from one format and timezone
 * to another. It uses SimpleDateFormat class and java locales and java
 * timezones to make this transformation. Default input is always used as the
 * input value for this transformer. Date format of the input is given in the
 * fields <i>oldformat</i>,[<i>oldlocale</i>,<i>oldtimezone</i>] and new
 * format of the date is given in field <i>newformat</i>,[<i>newlocale</i>,<i>newtimezone</i>].
 * Timezone change is optional. Consult javadocs of SimpleDateFormat class for
 * further information about date formats. <br>
 * <br>
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>dateformat</td>
 * </tr>
 * <tr>
 * <td>source</td>
 * <td>REQUIRED</td>
 * </tr>
 * <tr>
 * <td>target</td>
 * <td>REQUIRED</td>
 * </tr>
 * </table> <br />
 * 
 * Parameters for this transformation are:<br />
 * <br />
 * <table border="1">
 * <tr>
 * <td>oldformat</td>
 * <td>REQUIRED</td>
 * <td>Old time format. For example yyyyMMddHHmmss</td>
 * </tr>
 * <tr>
 * <td>newformat</td>
 * <td>REQUIRED</td>
 * <td>New time format. For example yyyy-MM-dd</td>
 * </tr>
 * <tr>
 * <td>oldlocale</td>
 * <td>OPTIONAL</td>
 * <td>Java locale of the old date. For example sv</td>
 * </tr>
 * <tr>
 * <td>oldlocalefield</td>
 * <td>OPTIONAL</td>
 * <td>Field where the java locale of the old date is read.</td>
 * </tr>
 * <tr>
 * <td>newlocale</td>
 * <td>OPTIONAL</td>
 * <td>Java locale of the new date. For example sv</td>
 * </tr>
 * <tr>
 * <td>newlocalefield</td>
 * <td>OPTIONAL</td>
 * <td>Field where the java locale of the new date is read.</td>
 * </tr>
 * <tr>
 * <td>oldtimezone</td>
 * <td>OPTIONAL</td>
 * <td>Offset of the old date. For example +0100</td>
 * </tr>
 * <tr>
 * <td>oldtimezonefield</td>
 * <td>OPTIONAL</td>
 * <td>Field where the offset of the old date is read.</td>
 * </tr>
 * <tr>
 * <td>newtimezone</td>
 * <td>OPTIONAL</td>
 * <td>Offset of the new date. For example +0100</td>
 * </tr>
 * <tr>
 * <td>newtimezonefield</td>
 * <td>OPTIONAL</td>
 * <td>Field where the offset of the new date is read.</td>
 * </tr>
 * </table> <br />
 * 
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Jarno Savinen, Tuomas Lemminkainen
 */
public class DateFormat implements Transformation {

  private String src = null;
  private String name;
  private String tgt = null;

  private String oldLocale = "";

  private String oldFormat = "";

  private String oldTimezone = "";

  private String oldFormatPar = "";

  private String oldLocalePar = "";

  private String oldTimezonePar = "";

  private String newLocale = "";

  private String newFormat = "";

  private String newTimezone = "";

  private String newFormatPar = "";

  private String newLocalePar = "";

  private String newTimezonePar = "";

  private static HashMap tzLookupCache = new HashMap(10);

  private static final String KEY_MID_STR = "-";

  private final LimitedSizeStringCache cache = new LimitedSizeStringCache();

  DateFormat() {
  }

  public void transform(final Map data, final Logger log) {

    final String input = (String) data.get(src);
    if (input == null) {
      return;
    }

    SimpleDateFormat oldSDFormat = null;
    SimpleDateFormat newSDFormat = null;

    String t_oldTimezone = oldTimezone;
    String t_newTimezone = newTimezone;

    if (oldTimezonePar.length() > 0) {
      t_oldTimezone = (String) data.get(oldTimezonePar);
    }

    if (newTimezonePar.length() > 0) {
      t_newTimezone = (String) data.get(newTimezonePar);
    }

    log.finest("DateFormat: TimeZone " + t_oldTimezone + " -> " + t_newTimezone);

    String cache_key = null;

    try {
      String t_oldFormat = oldFormat;
      String t_newFormat = newFormat;

      if (oldFormatPar.length() > 0) {
        t_oldFormat = (String) data.get(oldFormatPar);

        if (t_oldFormat == null) {
          throw new ExpectedException("No value for oldFormat field " + oldFormatPar);
        }
      }

      if (newFormatPar.length() > 0) {
        t_newFormat = (String) data.get(newFormatPar);

        if (t_newFormat == null) {
          throw new ExpectedException("No value for oldFormat field " + newFormatPar);
        }
      }

      log.finest("DateFormat: Format " + t_oldFormat + " -> " + t_newFormat);

      // Formats are ready

      String t_oldLocale = oldLocale;
      String t_newLocale = newLocale;

      if (oldLocalePar.length() > 0) {
        t_oldLocale = (String) data.get(oldLocalePar);
      }

      if (newLocalePar.length() > 0) {
        t_newLocale = (String) data.get(newLocalePar);
      }

      log.finest("DateFormat: Locale " + t_oldLocale + " -> " + t_newLocale);

      // Parameters [ TimeZone Format Locale ] ready... Accessing cache.

      cache_key = input + KEY_MID_STR + t_oldFormat + KEY_MID_STR + t_newFormat + KEY_MID_STR + t_oldTimezone
          + KEY_MID_STR + t_newTimezone + KEY_MID_STR + t_oldLocale + KEY_MID_STR + t_newLocale;

      final String cached = (String) cache.get(cache_key);

      if (null != cached) {
        log.finest("DateFormat.transform: Found date from cache. Value = " + cached);
        data.put(tgt, cached);
        return;
      }

      // if oldLocale is empty or null we use "local" locale
      if (t_oldLocale != null && t_oldLocale.length() > 0) {
        final Locale ol = new Locale(t_oldLocale);
        oldSDFormat = new SimpleDateFormat(t_oldFormat, ol);
      } else {
        oldSDFormat = new SimpleDateFormat(t_oldFormat);
      }

      // if oldTimezone is empty or null we use "local" Timezone
      if (t_oldTimezone != null && t_oldTimezone.length() > 0) {

        if (!t_oldTimezone.startsWith("GMT")) {
          t_oldTimezone = "GMT" + t_oldTimezone;
        }

        TimeZone tz = (TimeZone) tzLookupCache.get(t_oldTimezone);
        if (tz == null) {
          tz = TimeZone.getTimeZone(t_oldTimezone);
          tzLookupCache.put(t_oldTimezone, tz);
        }

        oldSDFormat.setTimeZone(tz);
      }

      // if newLocale is empty or null we use "local" locale
      if (t_newLocale != null && t_newLocale.length() > 0) {
        final Locale nl = new Locale(t_newLocale);
        newSDFormat = new SimpleDateFormat(t_newFormat, nl);
      } else {
        newSDFormat = new SimpleDateFormat(t_newFormat);
      }

      // if newTimezone is empty or null we use "local" Timezone
      if (t_newTimezone != null && t_newTimezone.length() > 0) {

        if (!t_newTimezone.startsWith("GMT")) {
          t_newTimezone = "GMT" + t_newTimezone;
        }

        TimeZone tz = (TimeZone) tzLookupCache.get(t_newTimezone);
        if (tz == null) {
          tz = TimeZone.getTimeZone(t_newTimezone);
          tzLookupCache.put(t_newTimezone, tz);
        }

        newSDFormat.setTimeZone(tz);
      }

    } catch (ExpectedException ice) {
      log.info("DateFormat: Error while figuring out parameters: " + ice.getMessage());
    } catch (Exception e) {
      log.log(Level.INFO, "DateFormat: Error while figuring out parameters.", e);
    }

    try {

      final Date d = oldSDFormat.parse(input);
      final String output = newSDFormat.format(d);
      log.finest("DateFormat.transform: input " + input + " transformed to output " + output);

      if (cache_key != null) {
        cache.put(cache_key, output);
      }

      data.put(tgt, output);

    } catch (ParseException e) {
      log.fine("DateFormat: Invalid timestamp: \"" + input + "\" in field " + src);
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog)
      throws ConfigException {

    this.src = src;
    this.tgt = tgt;
    this.name = name;
    newFormatPar = props.getProperty("newformatfield", "");
    oldFormatPar = props.getProperty("oldformatfield", "");
    newLocalePar = props.getProperty("newlocalefield", "");
    oldLocalePar = props.getProperty("oldlocalefield", "");
    newTimezonePar = props.getProperty("newtimezonefield", "");
    oldTimezonePar = props.getProperty("oldtimezonefield", "");

    oldFormat = props.getProperty("oldformat", "");
    newFormat = props.getProperty("newformat", "");
    oldLocale = props.getProperty("oldlocale", "");
    oldTimezone = props.getProperty("oldtimezone", "");
    newLocale = props.getProperty("newlocale", "");
    newTimezone = props.getProperty("newtimezone", "");

    if ((oldFormat == null && oldFormatPar.length() < 1) || (newFormat == null && newFormatPar.length() < 1)) {
      throw new ConfigException("Parameters oldformat and newformat or newFormatPar and oldFormatPar has to be defined");
    }

  }

  public class ExpectedException extends Exception {

    public ExpectedException(final String msg) {
      super(msg);
    }
  };
  public String getSource() throws Exception {
    // TODO Auto-generated method stub
    return src;
  }

  public String getTarget() throws Exception {
    // TODO Auto-generated method stub
    return tgt;
  }
  
  public String getName() throws Exception {
    // TODO Auto-generated method stub
    return name;
  }
}
