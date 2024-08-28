package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

/**
 * This transformer performs switch operations. If <i>source</i> field equals to value defined using
 * element <i>old</i> the value defined in element <i>new</i> is set as target field.<br>
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>switch</td>
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
 *   <td>old</td>
 *   <td>REQUIRED</td>
 *   <td>Value to compare <i>source</i> to. If <i>old</i> equals <i>source</i> then <i>new</i> is set to <i>target</i>.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * Copyright by Distocraft 2004 <br />
 * All rights reserved
 * 
 * @author Antti Laurila
 */
public class Switch implements Transformation {

  private String src = null;
  private String tgt = null;
  private String name;
  private String newValue = "";
  private String oldValue = "";

  public void transform(final Map data, final Logger clog) {
    final String input = (String) data.get(src);

    if (input == null) {
      return;
    }

    if (input.equals(oldValue)) {
      data.put(tgt, newValue);
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) throws ConfigException {
    this.tgt = tgt;
    this.src = src;
    this.name = name;
    
    oldValue = props.getProperty("old");
    newValue = props.getProperty("new");
    
    if(oldValue == null || newValue == null) {
      throw new ConfigException("Parameters old and new have to be defined");
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
