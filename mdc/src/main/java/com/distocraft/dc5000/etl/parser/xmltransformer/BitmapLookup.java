package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

/**
 * <br>
 * BitmapLookup transformation <br>
 * <br>
 * Transformation reads a table (named by tablename) from DB with two columns. Tables first column contains names and
 * second bitmasks in desimal format (integers). <br>
 * Thease bitmask values are matched against the value given in source. If the given source value matches the bits
 * described in bitmask (read from DB) the name column of the table is added (comma delimited) to result string that is
 * returned in target field.<br>
 * <br>
 * <br>
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>bitmaplookup</td>
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
 * <td>tablename</td>
 * <td>REQUIRED</td>
 * <td> Name of the table where the bitmask values and names are retrieved.</td>
 * </tr>
 * </table> <br />
 * 
 * 
 * 
 * Copyright by Distocraft 2007 <br />
 * All rights reserved
 * 
 * @author Jarno Savinen
 */
public class BitmapLookup implements Transformation {

  private LinkedHashMap lookupMap = null;

  private RockFactory rock = null;

  private String src;

  private String tgt;
  private String name;

  BitmapLookup(final RockFactory rock) {
    this.rock = rock;
  }

  public void transform(final Map data, final Logger clog) {

    if (src != null && src.length() > 0) {

      final StringBuffer result = new StringBuffer();

      final String valueStr = (String) data.get(src);

      if (valueStr != null && valueStr.length() > 0) {

        final int value = Integer.parseInt(valueStr);
        
        boolean first = true;
        
        final Iterator iter = lookupMap.keySet().iterator();
        while (iter.hasNext()) {

          final String key = (String) iter.next();
          if ((value & Integer.parseInt(key)) > 0) {

            if (!first) {
              result.append(",");
            }

            first = false;
            result.append(lookupMap.get(key));
          }
        }

        if (result != null) {
          data.put(tgt, result.toString());
        }
      }
    }
  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger log) throws ConfigException {

    this.src = src;
    this.tgt = tgt;
    this.name = name;
    Connection connection = null;
    Statement s = null;
    ResultSet resultSet = null;
    lookupMap = new LinkedHashMap();

    final String tableName = props.getProperty("tablename");

    if (tableName == null) {
      throw new ConfigException("Parameter tablename has to be defined");
    }
      
    try {

      log.fine("BMLookup: Connecting database");
      
      connection = rock.getConnection();

      s = connection.createStatement();

      resultSet = s.executeQuery("SELECT SERVICE_NAME, SERVICE_NUMBER FROM " + tableName);

      int i = 0;

      while (resultSet.next()) {
        i++;
        lookupMap.put(resultSet.getString(2), resultSet.getString(1));
      }

      log.fine("BMLookup: Loaded " + i + " mapping rows.");

    } catch (Exception e) {

      throw new ConfigException("Database connection failed exceptionally", e);

    } finally {

      if (resultSet != null) {
        try {
          SQLWarning sqlw  = resultSet.getWarnings();
          
          if (sqlw != null) {
            log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            while ((sqlw = sqlw.getNextWarning()) != null) {
              log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            }
          }
        } catch (Throwable t) {
        }

        try {
          resultSet.close();
        } catch (Throwable t) {
        }
      }

      if (s != null) {
        try {
          SQLWarning sqlw  = s.getWarnings();
          if (sqlw != null) {
            log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            while ((sqlw = sqlw.getNextWarning()) != null) {
              log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            }
          }
        } catch (Throwable t) {
        }

        try {
          s.close();
        } catch (Throwable t) {
        }
      }

      if (connection != null) {
        try {
          SQLWarning sqlw  = connection.getWarnings();
          if (sqlw != null) {
            log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            while ((sqlw = sqlw.getNextWarning()) != null) {
              log.log(Level.FINEST, "BMLookup: SQL warning on resultSet", sqlw);
            }
          }
        } catch (Throwable t) {
        }

        try {
          connection.clearWarnings();
        } catch (Exception e) {
        }
      }

    }
    rock = null;
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