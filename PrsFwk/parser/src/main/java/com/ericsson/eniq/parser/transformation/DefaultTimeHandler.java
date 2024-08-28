package com.ericsson.eniq.parser.transformation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.logging.log4j.Logger;

/**
 * This transformer class reads a datetime in specified format from <i>source</i>
 * and extracts it to following columns:
 * <ul>
 * <li>YEAR_ID</li>
 * <li>MONTH_ID</li>
 * <li>DAY_ID</li>
 * <li>HOUR_ID</li>
 * <li>MIN_ID</li>
 * <li>DATETIME_ID</li>
 * <li>DATE_ID</li>
 * </ul>
 * 
 * 
 * <br />
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>defaulttimehandler</td>
 * </tr>
 * <tr>
 * <td>source</td>
 * <td>REQUIRED</td>
 * </tr>
 * <tr>
 * <td>target</td>
 * <td>NOT USED</td>
 * </tr>
 * </table> <br />
 * 
 * Parameters for this transformation are:<br />
 * <br />
 * <table border="1">
 * <tr>
 * <td>oldformat</td>
 * <td>OPTIONAL</td>
 * <td>The timestamp format of <i>source</i> field. For example
 * yyyyMMddHHmmss.</td>
 * </tr>
 * <tr>
 * <td>oldlocale</td>
 * <td>OPTIONAL</td>
 * <td>The locale of <i>source</i> field.</td>
 * </tr>
 * <tr>
 * <td>oldtimezone</td>
 * <td>OPTIONAL</td>
 * <td>The timezone of <i>source</i> field.</td>
 * </tr>
 * <tr>
 * <td>newlocale</td>
 * <td>OPTIONAL</td>
 * <td>The locale of target fields.</td>
 * </tr>
 * <tr>
 * <td>newtimezone</td>
 * <td>OPTIONAL</td>
 * <td>The timezone of target fields.</td>
 * </tr>
 * <tr>
 * <td>newformat</td>
 * <td>OPTIONAL</td>
 * <td>The timestamp format of target fields.</td>
 * </tr>
 * </table> <br />
 * 
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class DefaultTimeHandler implements Transformation {

  private String src = null;
  private String name;
  private String oldFormat;

  private String oldLocale;

  private String oldZone;

  private String newLocale;

  private String newZone;

  private final LimitedSizeStringCache cache = new LimitedSizeStringCache();


  public void transform(final Map data, final Logger log) {

    final String input = (String) data.get(src);

    if (input == null) {
      return;
    }

    final String[] cached = (String[]) cache.get(input);
        
    if (cached != null) {
      data.put("YEAR_ID", cached[0]);
      data.put("MONTH_ID", cached[1]);
      data.put("DAY_ID", cached[2]);
      data.put("HOUR_ID", cached[3]);
      data.put("MIN_ID", cached[4]);
      data.put("DATE_ID", cached[5]);
      data.put("DATETIME_ID", cached[6]);

      if (log.isDebugEnabled()) {
        log.debug("DefaultTimeHandler.transform returned cached " + data);
      }

      return;
    }

    try {

      final SimpleDateFormat old_sdf;

      if (oldLocale == null) {
        old_sdf = new SimpleDateFormat(oldFormat);
      } else {
        final Locale ol = new Locale(oldLocale);
        old_sdf = new SimpleDateFormat(oldFormat, ol);
      }

      if (oldZone != null) {
        old_sdf.setTimeZone(TimeZone.getTimeZone(oldZone));
      }

      final Date date = old_sdf.parse(input);

      final Calendar new_cal;

      if (newLocale == null) {
        new_cal = new GregorianCalendar();
      } else {
        final Locale nl = new Locale(newLocale);
        new_cal = new GregorianCalendar(nl);
      }

      if (newZone != null) {
        new_cal.setTimeZone(TimeZone.getTimeZone(newZone));
      }

      new_cal.setTime(date);

      final String[] forcache = new String[7];

      data.put("YEAR_ID", String.valueOf(new_cal.get(Calendar.YEAR)));
      forcache[0] = String.valueOf(new_cal.get(Calendar.YEAR));
      data.put("MONTH_ID", String.valueOf(new_cal.get(Calendar.MONTH) + 1));
      forcache[1] = String.valueOf(new_cal.get(Calendar.MONTH) + 1);
      data.put("DAY_ID", String.valueOf(new_cal.get(Calendar.DAY_OF_MONTH)));
      forcache[2] = String.valueOf(new_cal.get(Calendar.DAY_OF_MONTH));
      data.put("HOUR_ID", String.valueOf(new_cal.get(Calendar.HOUR_OF_DAY)));
      forcache[3] = String.valueOf(new_cal.get(Calendar.HOUR_OF_DAY));
      data.put("MIN_ID", String.valueOf(new_cal.get(Calendar.MINUTE)));
      forcache[4] = String.valueOf(new_cal.get(Calendar.MINUTE));

      // yyyy-MM-dd

      final StringBuffer sb = new StringBuffer("");
      sb.append(new_cal.get(Calendar.YEAR)).append("-");

      if (new_cal.get(Calendar.MONTH) < 9) {
        sb.append(0);
      }
      sb.append(new_cal.get(Calendar.MONTH) + 1).append("-");

      if (new_cal.get(Calendar.DAY_OF_MONTH) < 10) {
        sb.append(0);
      }
      sb.append(new_cal.get(Calendar.DAY_OF_MONTH));

      data.put("DATE_ID", sb.toString());

      forcache[5] = sb.toString();

      sb.append(" ");

      if (new_cal.get(Calendar.HOUR_OF_DAY) < 10) {
        sb.append(0);
      }
      sb.append(new_cal.get(Calendar.HOUR_OF_DAY)).append(":");

      if (new_cal.get(Calendar.MINUTE) < 10) {
        sb.append(0);
      }
      sb.append(new_cal.get(Calendar.MINUTE)).append(":");

      if (new_cal.get(Calendar.SECOND) < 10) {
        sb.append(0);
      }
      sb.append(new_cal.get(Calendar.SECOND));

      data.put("DATETIME_ID", sb.toString());

      forcache[6] = sb.toString();

      log.debug("DefaultTimeHandler.transform forcache = " + forcache);
      log.debug("DefaultTimeHandler.transform returned data" + data);
            
      cache.put(input, forcache);

    } catch (ParseException e) {
      log.info("Invalid timestamp: \"" + input + "\" in column " + src);
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog)
      throws ConfigException {
    this.name = name;
    this.src = src;
    // tgt not needed

    oldFormat = props.getProperty("oldformat");

    if (oldFormat == null) {
      throw new ConfigException("Parameter oldformat has to be defined");
    }

    oldLocale = props.getProperty("oldlocale", null);
    oldZone = props.getProperty("oldtimezone", null);

    newLocale = props.getProperty("newlocale", null);
    newZone = props.getProperty("newtimezone", null);

  }
  public String getSource() throws Exception {
    // TODO Auto-generated method stub
    return src;
  }

  public String getTarget() throws Exception {
    // TODO Auto-generated method stub   
    return "YEAR_ID,MONTH_ID,DAY_ID,HOUR_ID,MIN_ID,DATETIME_ID,DATE_ID";
  }
  
  public String getName() throws Exception {
    // TODO Auto-generated method stub
    return name;
  }
}
