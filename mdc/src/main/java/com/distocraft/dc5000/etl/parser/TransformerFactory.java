package com.distocraft.dc5000.etl.parser;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.parser.xmltransformer.DBTransformer;

/**
 * Created on Jan 31, 2005
 * 
 * @author lemminkainen
 */
public final class TransformerFactory {

  public static Transformer create(final String tID, final RockFactory rf, final RockFactory reprf)
      throws Exception {

    if (tID == null) { // transformer not defined
      return null;
    }
      
    return new DBTransformer(tID, rf, reprf);

  }

}
