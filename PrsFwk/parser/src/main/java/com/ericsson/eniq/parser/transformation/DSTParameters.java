package com.ericsson.eniq.parser.transformation;

import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.logging.log4j.Logger;

/**
 * This transformer returns the amount of time to be added to local standard
 * time to get local wall clock time.
 * 
 * Parameters for this transformation are:<br />
 * <br />
 * <tr>
 *   <td>timezone</td>
 *   <td>OPTIONAL</td>
 *   <td>ID of timezone used (uses current timezone of host if not defined)</td>
 * </tr>
 * </table>
 * Copyright by Ericsson 2007 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class DSTParameters implements Transformation {

  private String tgt = null;
  private TimeZone tz = TimeZone.getDefault();
  private String name = "";
  
  public void configure(final String name, final String srcKey, final String tgtKey, final Properties props, final Logger clog) throws ConfigException {
    this.name = name;
    this.tgt = tgtKey;
    
    final String szone = props.getProperty("timezone");
    if (szone != null) {
      tz = TimeZone.getTimeZone(szone);
    }
  }

  public void transform(final Map data, final Logger clog) throws Exception {
    final int dst = (tz.inDaylightTime(new Date(System.currentTimeMillis()))) ? 1 : 0;
    data.put(tgt, String.valueOf(dst));
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
