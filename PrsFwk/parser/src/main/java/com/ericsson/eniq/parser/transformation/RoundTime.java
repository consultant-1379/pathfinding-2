package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Properties;

/**
 * This transformer class will round a date value to the nearest minute or hour.
 * Fields <i>minute</i> and <i>hour</i> defines which (or both) is used. Default 
 * input is always used as the input for the date string. The format of the input date string 
 * is given with the field <i>format</i>. Consult javadocs of class SimpleDateFormat for further 
 * information about date formats.<br/>
 * <br/>
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>roundtime</td>
 * </tr>
 * <tr>
 *   <td>source</td>
 *   <td>REQUIRED</td>
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
 *   <td>Format of the input date string. For example yyyyMMddHHmmss.</td>
 * </tr>
 * <tr>
 *   <td>minute</td>
 *   <td>OPTIONAL</td>
 *   <td>If contains any data, the date in <i>source</i> is rounded to the nearest minute.</td>
 * </tr>
 * <tr>
 *   <td>hour</td>
 *   <td>OPTIONAL</td>
 *   <td>If contains any data, the date in <i>source</i> is rounded to the nearest hour.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * 
 * Copyright by Distocraft 2005 <br />
 * All rights reserved
 * 
 * @author Jarno Savinen
 */
public class RoundTime implements Transformation {

  private String src = null;
  private String tgt = null;
  private String name;
  private String format = null;

  private boolean minute = false;
  private boolean hour = false;

  public void transform(final Map data, final Logger clog) {

    final String input = (String)data.get(src);
    
    if(input == null) {
      return;
    }
    
    final SimpleDateFormat df = new SimpleDateFormat(format);
    
    try {
    	
    	// get datetime and put in a date
    	final Date date = df.parse(input);
      
    	// put date to the calendar
      final Calendar rounded = new GregorianCalendar();
      rounded.setTime(date);      
        
      // if we round seconds off
      if (minute) {
        	
        // divide seconds by 30 so that <30 is 0 and >30 is 1 and add it to the minute
        rounded.add(Calendar.MINUTE,rounded.get(Calendar.SECOND)/30);
        rounded.set(Calendar.SECOND,0);
      }

      // if we round minutes off
		  if (hour) {

			  //divide minutes by 30 so that <30 is 0 and >30 is 1 and add it to the hour
			  rounded.add(Calendar.HOUR,rounded.get(Calendar.MINUTE)/30);
			  rounded.set(Calendar.SECOND,0);
			  rounded.set(Calendar.MINUTE,0);
 		  }
		
      data.put(tgt,df.format(rounded.getTime()));
      
    } catch (Exception e) {
      clog.info("RoundDate transformation error", e);
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) throws ConfigException {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
    format = props.getProperty("format");
    
    if(format == null) {
      throw new ConfigException("Parameter format has to be defined");
    }
      
    if (props.getProperty("minute") != null) {
      minute = true;
    }

    if (props.getProperty("hour") != null) {
    	hour = true;
    }
    
    if(!minute && !hour) {
      throw new ConfigException("Parameter minute and/or hour has to be defined");
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
