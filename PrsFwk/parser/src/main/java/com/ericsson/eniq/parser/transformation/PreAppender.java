package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

/**
 * This transformer appends a value to the beginning of an input field. Appended
 * value can be either fixed string or value can be fetched from another field.
 * <br>
 * <br>
 * Examples:<br>
 * <br>
 * Appending field X as prefix to field Y and place result into field Z.
 * Configuration: sourcefield=X, targetfield=Z, parameters: field=Y<br>
 * <code>X=BAR Y=FOO --(transform)--> X=BAR Y=FOO Z=FOOBAR</code> <br>
 * <br>
 * Appending string "_foobar" as prefix to field X and place result into field
 * Z. Configuration: sourcefield=X, targetfield=Z, parameters: fixed=_foobar<br>
 * <code>X=FOOBAR --(transform)--> X=FOOBAR Z=FOOBAR_foobar
 * <br>
 * <br>
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>preappender</td>
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
 *   <td>field</td>
 *   <td>OPTIONAL</td>
 *   <td>Name of the field which is preappended to the field <i>source</i>.</td>
 * </tr>
 * <tr>
 *   <td>fixed</td>
 *   <td>OPTIONAL</td>
 *   <td>Fixed value which is preappended to the field <i>source</i>.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * 
 * Copyright by Distocraft 2004-6 <br />
 * All rights reserved
 * 
 * @author laurila,lemminkainen
 */
public class PreAppender implements Transformation {

  private String src = null;
  private String name;
  private String tgt = null;

  private String appendField = null;

  private String appendFixed = null;


  public void transform(final Map data, final Logger clog) {
    final String input = (String) data.get(src);

    if (input == null) {
      return;
    }

    if (appendFixed != null) {
      data.put(tgt, appendFixed + input);
    } else if (appendField != null) {
      final String s = (String) data.get(appendField);
      if (s != null && s.length() > 0) {
        data.put(tgt, s + input);
      } else {
        data.put(tgt, input);
      }
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) throws ConfigException {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
    appendField = props.getProperty("field");
    appendFixed = props.getProperty("fixed");

    if (appendField == null && appendFixed == null) {
      throw new ConfigException("Parameter field or fixed has to be defined");
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
