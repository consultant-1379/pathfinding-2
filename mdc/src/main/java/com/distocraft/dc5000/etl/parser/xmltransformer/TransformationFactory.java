package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.alarm.AlarmTransformation;

public class TransformationFactory {

  private static final String CONVERTIPADDRESS = "convertipaddress";
  private static final String HASH_ID = "hashid";

  private TransformationFactory() {

  }

  /**
   * Factory Method that produces actual instances of transformers
   */
  public static Transformation getTransformation(final String tname, final String src, final String tgt,
      final Properties conf, final RockFactory rock, final Logger log) {

    log.finest("Creating transformation " + tname + " (" + src + "->" + tgt + ")");

    final Enumeration e = conf.keys();
    while (e.hasMoreElements()) {
      final String key = (String) e.nextElement();
      log.finest("    parameter: " + key + "=" + conf.getProperty(key));
    }

    if (tname == null) {
      log.config("Transformation (" + src + "->" + tgt + ") has no type attribute. Skipping");
      return null;
    }

    if (src == null) {
      log.config("Transformation " + tname + "(? -> " + tgt + ") has no source attribute. Skipping");
      return null;
    }

    if (tgt == null) {
      log.config("Transformation " + tname + "(" + src + " -> ?) has no target attribute. Skipping");
      return null;
    }

    Transformation t = null;

    try {

      if (tname.equalsIgnoreCase("lookup")) {
        t = new Lookup();
      } else if (tname.equalsIgnoreCase("databaselookup")) {
        t = new DatabaseLookup();
      } else if (tname.equalsIgnoreCase("bitmaplookup")) {
        t = new BitmapLookup(rock);
      } else if (tname.equalsIgnoreCase("condition")) {
        t = new Condition();
      } else if (tname.equalsIgnoreCase("radixConverter")) {
        t = new RadixConverter();
      } else if (tname.equalsIgnoreCase("dateformat")) {
        t = new DateFormat();
      } else if (tname.equalsIgnoreCase("defaulttimehandler")) {
        t = new DefaultTimeHandler();
      } else if (tname.equalsIgnoreCase("fixed")) {
        t = new Fixed();
      } else if (tname.equalsIgnoreCase("postappender")) {
        t = new PostAppender();
      } else if (tname.equalsIgnoreCase("preappender")) {
        t = new PreAppender();
      } else if (tname.equalsIgnoreCase("reducedate")) {
        t = new ReduceDate();
      } else if (tname.equalsIgnoreCase("roundtime")) {
        t = new RoundTime();
      } else if (tname.equalsIgnoreCase("switch")) {
        t = new Switch();
      } else if (tname.equalsIgnoreCase("copy")) {
        t = new Copy();
      } else if (tname.equalsIgnoreCase("propertytokenizer")) {
        t = new PropertyTokenizer();
      } else if (tname.equalsIgnoreCase("fieldtokenizer")) {
        t = new FieldTokenizer();
      } else if (tname.equalsIgnoreCase("calculation")) {
        t = new Calculation();
      } else if (tname.equalsIgnoreCase("currenttime")) {
        t = new CurrentTime();
      } else if (tname.equalsIgnoreCase("alarm")) {
        t = new AlarmTransformation();
      } else if (tname.equalsIgnoreCase("dstparameters")) {
        t = new DSTParameters();
      } else if (tname.equalsIgnoreCase(HASH_ID)) {
        t = new HashId();
      } else if (tname.equalsIgnoreCase(CONVERTIPADDRESS)) {
        t = new ConvertIpAddress();
      } else if (tname.equalsIgnoreCase("roptime")) {
        t = new ROPTime();
      } else if (tname.equalsIgnoreCase("configlookup")) {
    	t = new ConfigLookup();
      } else {
        log.config("Unknown transformation type " + tname);
        return null;
      }

      t.configure(tname, src, tgt, conf, log);

    } catch (ConfigException ex) {
      log.config("Transformation " + tname + "(" + src + " -> " + tgt + "): " + ex.getMessage());
      final Throwable cause = ex.getCause();
      if (cause != null) {
        log.log(Level.FINE, "Error caused", ex);
      }
    } catch (Exception ex) {
      log.log(Level.CONFIG, "Transformation " + tname + "(" + src + " -> " + tgt + "): Exceptional failure", ex);
    }

    return t;

  }

}
