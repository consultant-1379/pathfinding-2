package com.ericsson.eniq.parser.transformation;

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

/**
 * This transformer copies <i>source</i> field into <i>target</i> field. 
 * <br/>
 * <br/>
 * 
 * <table border="1">
 * <tr>
 *   <td>copy</td>
 *   <td>dateformat</td>
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
 * This transformer has no parameters.<br />
 * <br />
 * 
 * 
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class Copy implements Transformation {

  private String src = null;
  private String tgt = null;
  private String name;
  
  
  public void transform(final Map data, final Logger clog) {
    final String input = (String)data.get(src);
    
    if(input != null) {
      data.put(tgt,input);
    }
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
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
