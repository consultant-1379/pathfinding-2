package com.distocraft.dc5000.etl.parser;

import java.util.HashMap;
import java.util.Properties;

import ssc.rockfactory.RockFactory;

/**
 * Object that is used for information sharing on a parser execution.
 * 
 * @author lemminkainen
 * 
 */
public class ParseSession {

  private final long sessionID;

  private final Properties parserConf;

  private final HashMap transformerCache = new HashMap();

  public ParseSession(long sessionID, Properties conf) {
    this.sessionID = sessionID;
    this.parserConf = conf;
  }

  Transformer getTransformer(final String tID, final RockFactory rf, final RockFactory reprf) throws Exception {

    if (tID == null || tID.length() <= 0) {
      return null;
    }

    Transformer t = (Transformer) transformerCache.get(tID);

    if (t == null) { // cache miss

      t = TransformerFactory.create(tID, rf, reprf);

      transformerCache.put(tID, t);

    }

    return t;

  }

  public String getParserParameter(final String key) {
    return parserConf.getProperty(key);
  }

  long getSessionID() {
    return sessionID;
  }

  /**
   * Clears ParseSession caches
   */
  void clear() {
    transformerCache.clear();
  }

}
