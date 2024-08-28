package com.ericsson.eniq.parser.transformation;

import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

/**
 * This transformer substracts a portion of a string according to a regular expression pattern. The
 * regular expression pattern is given either with <i>pattern</i>, <i>ipattern</i> or
 * <i>epattern</i>. If <i>pattern</i> is used, transformer will return the first group of the matching
 * pattern. In case of <i>ipattern</i>, transformer will return the matching part of the string from the
 * beginning of the string. <i>epattern</i> is otherwise similar to the <i>ipattern</i>, but the part of the string
 * where matching ends is returned.<br />
 * <br />
 * 
 * <br />
 * <table border="1">
 * <tr>
 *   <td>type</td>
 *   <td>lookup</td>
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
 *   <td>pattern</td>
 *   <td>OPTIONAL</td>
 *   <td>Regular expression pattern to return the first group of the matching pattern.</td>
 * </tr>
 * <tr>
 *   <td>ipattern</td>
 *   <td>OPTIONAL</td>
 *   <td>Regular expression pattern to return the matching part from the beginning of the string.</td>
 * </tr>
 * <tr>
 *   <td>epattern</td>
 *   <td>OPTIONAL</td>
 *   <td>Regular expression pattern to return the matching part to the end of the string.</td>
 * </tr>
 * </table>
 * <br />
 * 
 * 
 * Copyright by Distocraft 2004 <br>
 * All rights reserved
 * 
 * @author Antti Laurila
 */
public class Lookup implements Transformation {

  private String src = null;
  private String name;
  private String tgt = null;

  private Pattern ipatt = null;

  private Pattern epatt = null;

  private Pattern patt = null;


  public void transform(final Map data, Logger logger) {
    String retval = null;

    final String input = (String) data.get(src);

    if (input == null) {
      return;
    }

    if (ipatt != null) {
      final Matcher m = ipatt.matcher(input);
      if (m.find()) {
        retval = m.group();
      }
    } else if (epatt != null) {
      final Matcher m = epatt.matcher(input);
      if (m.find()) {
        retval = input.substring(m.end());
      } else {
        retval = input;
      }
    } else if (patt != null) {
      final Matcher m = patt.matcher(input);
      if (m.matches()) {
        retval = m.group(1);
      }
    }

    if (retval != null) {
      data.put(tgt, retval);
    }
      
  }

  public void configure(final String name, final String srcKey, final String tgtKey, final Properties props, Logger logger) throws Exception {
    this.name = name;
    src = srcKey;
    tgt = tgtKey;

    if (props.getProperty("ipattern") != null) {
      ipatt = Pattern.compile(props.getProperty("ipattern"));
    }
    
    if (props.getProperty("epattern") != null) {
      epatt = Pattern.compile(props.getProperty("epattern"));
    }
    
    if (props.getProperty("pattern") != null) {
      patt = Pattern.compile(props.getProperty("pattern"));
    }
    
    if(ipatt == null && epatt == null && patt == null) {
      throw new Exception("One of these parameters must be defined: ipattern, epattern or pattern");
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
