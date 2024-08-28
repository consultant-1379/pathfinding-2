package com.ericsson.eniq.parser.transformation;

import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * Field tokenizer is used to tokenize delimited strings.
 * 
 * Example:<br/> 
 * 
 * If column <i>source</i> in data would have value &quot;A,B,C&quot; and <i>delim</i> would have valued &quot;,&quot; this transformation
 * would create following columns into data:<br>
 * <i>target</i>.0="A"<br>
 * <i>target</i>.1="B"<br>
 * <i>target</i>.2="C"<br>
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>fieldtokenizer</td>
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
 *   <td>delim</td>
 *   <td>REQUIRED</td>
 *   <td>Delimeter string to use in tokenizing delimeted string.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * 
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 */
public class FieldTokenizer implements Transformation {

  private String src = null;
  private String name;
  private String tgt = null;

  private String delim = null;


  public void transform(final Map data, final Logger clog) {
    final String input = (String) data.get(src);

    if (input == null || input.length() <= 0) {
      return;
    }

    final StringTokenizer tz = new StringTokenizer(input, delim);
    int i = 0;
    while (tz.hasMoreTokens()) {
      final String tok = tz.nextToken();
      data.put(tgt + "." + i, tok);
      i++;
    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
    delim = props.getProperty("delim", ",");

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
