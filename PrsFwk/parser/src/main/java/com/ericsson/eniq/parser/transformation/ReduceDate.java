package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * This transformer class will reduce a date value by the number of seconds
 * defined. Default input is always used as the input for the date string. The
 * format of the input date string is given with the field <i>format</i>.
 * Consult javadocs of class SimpleDateFormat for further information about date
 * formats. The number of seconds to be reduced from the date is given either
 * with the field <i>seconds</i> if seconds are given as a plain number or with
 * the field <i>secondsname</i> if the number of seconds is taked from a field.
 * <br/> <br/>
 * 
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>reducedate</td>
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
 * <td>format</td>
 * <td>REQUIRED</td>
 * <td>Format of the input date string. For example yyyyMMddHHmmss.</td>
 * </tr>
 * <tr>
 * <td>seconds</td>
 * <td>OPTIONAL</td>
 * <td>Number of seconds to reduce from the date given in <i>source</i>.</td>
 * </tr>
 * <tr>
 * <td>secondsfield</td>
 * <td>OPTIONAL</td>
 * <td>Name of the field to take the number of seconds to reduce from the date
 * given in <i>source</i>.</td>
 * </tr>
 * </table> <br />
 * 
 * Copyright by Distocraft 2005 <br />
 * All rights reserved
 * 
 * @author Timo Melkko
 */
public class ReduceDate implements Transformation {

  private String src = null;
  private String name;
  private String tgt = null;

  private String format = null;

  private long seconds = 0;

  private String secondsVar = null;


  public void transform(final Map data, final Logger clog) {
    final String input = (String) data.get(src);

    if (input == null) {
      return;
    }

    long t_seconds;

    if (secondsVar == null) {
      t_seconds = seconds;
    } else {
      t_seconds = Long.parseLong((String) data.get(secondsVar));
    }

    try {

      final SimpleDateFormat sdf = new SimpleDateFormat(format);

      long time = sdf.parse(input).getTime();

      time = time - 1000 * t_seconds;
      final Date date = new Date(time);

      data.put(tgt, sdf.format(date));

    } catch (Exception e) {
      clog.info("ReducateDate transformation error", e);
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog)
      throws ConfigException {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
    format = props.getProperty("format");
    secondsVar = props.getProperty("secondsfield");
    final String secs = props.getProperty("seconds");

    if (format == null) {
      throw new ConfigException("Parameter format have to be defined");
    }
      
    if (secondsVar == null && secs == null) {
      throw new ConfigException("Paremeter secondsfield or seconds have to be defined");
    }
      
    if (secs != null){
        try {
            seconds = Long.parseLong(secs);
          } catch (NumberFormatException nfe) {
            throw new ConfigException("Parameter seconds has illegal value \"" + secs + "\"");
          }
    }
  }
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
