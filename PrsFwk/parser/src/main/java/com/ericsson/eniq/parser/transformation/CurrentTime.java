package com.ericsson.eniq.parser.transformation;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.logging.log4j.Logger;

/**
 * This transformer class sets current date value to specified <i>target</i> column. Consult
 * javadocs of SimpleDateFormat class for further information about date
 * formats. <br>
 * <br>
 *  
 * <br>
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>currenttime</td>
 * </tr>
 * <tr>
 *   <td>source</td>
 *   <td>NOT USED</td>
 * </tr>
 * <tr>
 *   <td>target</td>
 *   <td>REQUIRED</td>
 * </tr>
 * </table>
 * <br />
 * 
 * Parameters for this transformation are:<br />
 * <br />
 * <table border="1">
 * <tr>
 *   <td>format</td>
 *   <td>REQUIRED</td>
 *   <td>Format of timestamp (SimpleDateFormat)</td>
 * </tr>
 * <tr>
 *   <td>locale</td>
 *   <td>OPTIONAL</td>
 *   <td>Locale used (uses current locale of host if not defined)</td>
 * </tr>
 * <tr>
 *   <td>timezone</td>
 *   <td>OPTIONAL</td>
 *   <td>Timezone used (uses current timezone of host if not defined)</td>
 * </tr>
 * </table>
 * <br />
 *
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class CurrentTime implements Transformation {

  private String tgt = null;
  private String name;
  private SimpleDateFormat format;


  public void transform(final Map data, final Logger clog) {

    final String output = format.format(new Date());
    data.put(tgt, output);

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) throws ConfigException {

    this.tgt = tgt;
    this.name = name;
    final String sformat = props.getProperty("format");

    if (sformat == null) {
      throw new ConfigException("Parameter format has to be defined");
    }
      
    final String slocale = props.getProperty("locale");
    final String szone = props.getProperty("timezone");

    if (slocale == null) {
      format = new SimpleDateFormat(sformat);
    } else {
      final Locale ol = new Locale(slocale);
      format = new SimpleDateFormat(sformat, ol);
      if (szone != null) {
        format.setTimeZone(TimeZone.getTimeZone(szone));
      }      
    }

  }
  public String getSource() throws Exception {
    // TODO Auto-generated method stub
    return null;
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
