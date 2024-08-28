package com.ericsson.eniq.parser.transformation;

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

/**
 * This transformer class offers condition functionality. Inputs for this transformation are
 * <i>factor(field)</i>, <i>result1(field)</i> and <i>result2(field)</i>. If the value
 * <i>source</i> field and <i>factor</i> are equal, transformer stores <i>result1</i> into target field, otherwise
 * <i>result2</i>. If <i>factor</i> or </i>result</i> field is missing, default value, empty string, is used for
 * missing field. Using <i>result</i> and <i>factor</i> field takes fixed string as values. Using resultfield
 * takes the values from the data map using defined keys.<br>
 * <br>
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>condition</td>
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
 *   <td>factor</td>
 *   <td>REQUIRED</td>
 *   <td>The field where <i>Source</i> is compared to.</td>
 * </tr>
 * <tr>
 *   <td>result1</td>
 *   <td>REQUIRED</td>
 *   <td>Value which is set to <i>Target</i> if <i>Source</i> and <i>factor</i> equals.</td>
 * </tr>
 * <tr>
 *   <td>result2</td>
 *   <td>REQUIRED</td>
 *   <td>Value which is set to <i>Target</i> if <i>Source</i> and <i>factor</i> do not equal.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * Copyright by Distocraft 2005 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class Condition implements Transformation {

  private String src;
  private String name;
  private String tgt;
  
  private String factor = null;
  private String result1 = null;
  private String result2 = null;
  
  private String factorfield = null;
  private String result1field = null;
  private String result2field = null;

  public void transform(final Map data, final Logger clog) {

    String inputValue = (String) data.get(src);
    if (inputValue == null) {
      inputValue = "";
    }

    String factorValue = factor;
    if (factorValue == null) {
      factorValue = (String) data.get(factorfield);
    }
    if (factorValue == null) {
      factorValue = "";
    }

    String resultValue = null;

    if (inputValue.equals(factorValue)) {
      if (result1 == null) {
        resultValue = (String) data.get(result1field);
      } else {
        resultValue = result1;
      }
    } else {
      if (result2 == null) {
        resultValue = (String) data.get(result2field);
      } else {
        resultValue = result2;
      }
    }

    if (resultValue != null) {
      data.put(tgt, resultValue);
    }
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {

    this.src = src;
    this.tgt = tgt;
    this.name = name;
    this.factor = props.getProperty("factor",null);
    this.result1 = props.getProperty("result1",null);
    this.result2 = props.getProperty("result2",null);
    
    this.factorfield = props.getProperty("factorfield",null);
    this.result1field = props.getProperty("result1field",null);
    this.result2field = props.getProperty("result2field",null);
  
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
