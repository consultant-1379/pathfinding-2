package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * This transformer transforms <i>source</i> field into <i>target</i>.<br> 
 * It will handle source as fromRadix based value and converts it to toRadix based value.<br>
 * For example fromRadix 10 toRadix 16 -> "10" to "a" <br>
 * or fromRadix 16 toRadix 10 -> "a" to "10" <br>
 * <br/>
 * <br/>
 * 
 * <table border="1">
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
 *   <td>fromRadix</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is 10</td>
 * </tr>
 * <tr>
 *   <td>toRadix</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is 16</td>
 * </tr>
 * </table>
 * <br />  
 * @author etogust
 */
public class RadixConverter implements Transformation {

  private String src = null;
  private String tgt = null;
  private String name = null;

  private int fromRadix;
  private int toRadix;
  private Logger log = null;
  
  
  public void transform(final Map data, final Logger clog) {
    final String input = (String)data.get(src);
    log = clog;
    
    if(input != null) {
      data.put(tgt,convert(input));
    }
  }

  /**
   * 
   * @param value
   * @return
   */
  private String convert (String value){
    if (log != null)
      log.debug("Trying to convert " + value + " radix " + fromRadix + " to radix " + toRadix);

    String result = "";
    try {
      Long val = Long.parseLong(value, fromRadix);
      result = Long.toString(val, toRadix);
    }
    catch (Exception ex){
      if (log != null)
        log.warn("Failed to convert " + value + " radix " + fromRadix + " to radix " + toRadix, ex);
      return result;
    }
    
    if (log != null)
      log.debug("Converted " + value + " to " + result + ".");
    
    return result;
  }
  
  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
  
    fromRadix = Integer.parseInt(props.getProperty("fromRadix", "10"));
    toRadix = Integer.parseInt(props.getProperty("toRadix", "16"));
  }
  
  public String getSource() throws Exception {
    return src;
  }
  public String getTarget() throws Exception {
    return tgt;
  }
  public String getName() throws Exception {
    return name;
  }  
  
}
