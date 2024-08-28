package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

/**
 * This transformer sets a fixed value into <i>target</i> field. The fixed value is
 * given using <i>value</i> field.<br>
 * 
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>fixed</td>
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
 *   <td>value</td>
 *   <td>REQUIRED</td>
 *   <td>The value to set to <i>target</i></td>
 * </tr>
 * </table>
 * <br />
 * 
 * </code> Copyright by Distocraft 2004-2005<br/> All rights reserved
 * 
 * @author Antti Laurila
 */
public class Fixed implements Transformation {

  private String tgt = null;
  private String name;
  private String fixedStr = null;


  public void transform(final Map data, final Logger clog) {
    data.put(tgt, fixedStr);
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) throws ConfigException {
    // src not needed
    this.tgt = tgt;
    this.name = name;
    fixedStr = props.getProperty("value");

    if (fixedStr == null) {
      throw new ConfigException("Parameter value has to be defined");
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
