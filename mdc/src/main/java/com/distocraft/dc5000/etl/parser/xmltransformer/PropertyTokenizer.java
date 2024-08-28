package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

/**
 * Field tokenizer is used to handle delimited name-value pair strings.
 * 
 * For example:<br />
 * If <i>source</i> has a value <code>&quot;A=1,B=2,C=3&quot;</code> 
 * and <i>delim</i> has a value <code>&quot;,&quot;</code> 
 * this transformation would create the following columns into data:<br />
 * <i>target</i>.A=&quot;1&quot;<br />
 * <i>target</i>.B=&quot;2&quot;<br />
 * <i>target</i>.C=&quot;3&quot;<br />
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>propertytokenizer</td>
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
 *   <td>Delimeter string used for the tokenization.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * 
 * Copyright by Distocraft 2006 <br />
 * All rights reserved
 * 
 * @author Tuomas Lemminkainen
 * Vishnu  - Fixed for TR:HL65144 ( Fault on propertytokenizer transformation ) on 20/Apr/2010 for Eniq 2.5
 */
public class PropertyTokenizer implements Transformation {

  private String src = null;
  private String tgt = null;
  private String tgts = "";
  private String name;
  private String delim = null;
  
  PropertyTokenizer() {    
  }
  
  public void transform(final Map data, final Logger clog) {
    final String input = (String)data.get(src);
    
    tgts = "";
    if(input == null) {
      return;
    }
    
    final StringTokenizer tz = new StringTokenizer(input,delim);
    
    while(tz.hasMoreTokens()) {
      final String perty = tz.nextToken();
      final int ix = perty.indexOf("=");
      
      if(ix > 0 && perty.length() > ix+1) {
        final String key = perty.substring(0,ix);
        final String value = perty.substring(ix+1);
               
        final String key_fixed = key.trim();
        final String value_fixed = value.trim();

        data.put(tgt+"."+key_fixed,value_fixed);
        tgts += tgt+"."+key_fixed+",";
      }
    }
   
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
    this.src = src;
    this.tgt = tgt;
    this.name = name;
    this.delim = props.getProperty("delim",",");
  }
  public String getSource() throws Exception {
    // TODO Auto-generated method stub
    return src;
  }

  public String getTarget() throws Exception {
    // TODO Auto-generated method stub
    return tgts;
  }
  public String getName() throws Exception {
    // TODO Auto-generated method stub
    return name;
  }
}
