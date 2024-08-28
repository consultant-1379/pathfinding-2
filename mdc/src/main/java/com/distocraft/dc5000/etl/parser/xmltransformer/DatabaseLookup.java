package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.cache.DBLookupCache;

/**
 * This transformer class performs database lookups. Matching is done by equating the value <i>source</i> field with
 * given database mapping. Database mapping is given as an sql clause that is supposed to return two columns, 1 name and
 * 2 value. If database mapping does not contain value of <i>source</i> field the data map remains unchanged. <br>
 * <!-- Example: <br/>This database lookup takes default value of the PERIOD_DURATION as input and matches it with the
 * DURATIONMIN field of the select clause. Corresponding value of TIMELEVEL is then saved as field PERIOD_DURATION<br>
 * <code>
 * <pre>
 *         
 *     &lt;transformation type=&quot;databaseLookup&quot; source=&quot;PERIOD_DURATION&quot; target=&quot;PERIOD_DURATION&quot;&gt;
 *        &lt;sql&gt;select DURATIONMIN, TIMELEVEL from DIM_TIMELEVEL where TABLELEVEL = 'RAW'&lt;/sql&gt;
 *     &lt;/transformation&gt;
 *          
 * </pre>
 * </code> -->
 * 
 * 
 * 
 * <br>
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>databaselookup</td>
 * </tr>
 * <tr>
 * <td>source</td>
 * <td>REQUIRED</td>
 * </tr>
 * <tr>
 * <td>target</td>
 * <td>REQUIRED</td>
 * </tr>
 * </table> <br />
 * 
 * Parameters for this transformation are:<br />
 * <br />
 * <table border="1">
 * <tr>
 * <td>sql</td>
 * <td>REQUIRED</td>
 * <td>An sql clause that returns two values. First column is name and second column is value.<br />
 * For example:<br />
 * <code>select DURATIONMIN, TIMELEVEL from DIM_TIMELEVEL where TABLELEVEL = 'RAW'</code><br />
 * DURATIONMIN is compared to <i>source</i> and if it equals, TIMELEVEL is set to <i>target</i>. </td>
 * </tr>
 * <tr>
 * <td>basetable</td>
 * <td>REQUIRED</td>
 * <td>This parameter defines basetables names that this databaselookup uses in DWH database. Basetable can be found in
 * table dwhrep.MeasurementTable column basetablename for measurement types and in table dwhrep.ReferenceTable column
 * typename for reference types. Multiple tables should be separated with comma.<br/> For example:<br/>
 * <code>DIM_TIMELEVEL</code> defines that this database lookup uses table with basetablename DIM_TIMELEVEL. </td>
 * </table> <br />
 * 
 * 
 * 
 * 
 * Copyright by Distocraft 2005 <br />
 * All rights reserved
 * 
 * @author Timo Melkko
 */
public class DatabaseLookup implements Transformation {

  private String sql = null;

  private String src;
  private String name;
  private String tgt;

  DatabaseLookup() {

  }

  public void transform(final Map data, final Logger clog) {

    final DBLookupCache dblc = DBLookupCache.getCache();

    final Map lookupMap = dblc.get(sql);

     // Construct these huge Strings only when logging level is finest
    
    if (clog.isLoggable(Level.FINEST)) {
      clog.finest("DatabaseLookup.transform: lookupMap = " + lookupMap);
      clog.finest("DatabaseLookup.transform: sql = " + sql);
      clog.finest("DatabaseLookup.transform: data = " + data);
      clog.finest("DatabaseLookup.transform: tgt = " + tgt);
    }
    
    final String lookedup = (String) lookupMap.get(data.get(src));

    clog.finest("DatabaseLookup.transform: lookedup = " + lookedup);

    if (lookedup != null) {
      data.put(tgt, lookedup);
    }
    
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger log)
      throws ConfigException {

    this.src = src;
    this.tgt = tgt;
    this.name = name;
    try {

      sql = props.getProperty("sql");
      String basetable = props.getProperty("basetable", null);
      log.log(Level.FINEST, "sql: " + sql);
      log.log(Level.FINEST, "basetable: " + basetable);
      DBLookupCache dblc = DBLookupCache.getCache();

      if (basetable == null) {

        dblc.add(sql, basetable);

      } else {

        String[] basetables = basetable.split(",");

        for (int i = 0; i < basetables.length; i++) {
          dblc.add(sql, basetables[i]);
          log.log(Level.FINEST, "Adding SQL " + sql + " to table " + basetables[i]);
        }

      }

    } catch (Exception e) {
      log.log(Level.WARNING, "DatabaseLookup: Error while configuring: "+sql,e);
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