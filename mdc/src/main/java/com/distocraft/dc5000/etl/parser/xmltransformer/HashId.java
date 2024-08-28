/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2011 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.ericsson.eniq.common.HashIdCreator;

/**
 * Used to generate a hash ID based on the source.
 * @author epaujor
 *
 */
public class HashId implements Transformation {

  private String source = null;

  private String target = null;

  private String hashIdName;

  @Override
  public void transform(final Map data, final Logger clog) throws NoSuchAlgorithmException, IOException {
    final String input = (String) data.get(source);

    if (input != null) {
      HashIdCreator hashIdCreator = new HashIdCreator();
      long hashId = hashIdCreator.hashStringToLongId(input);
      data.put(target, String.valueOf(hashId));
    }
  }

  @Override
  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
    this.source = src;
    this.target = tgt;
    this.hashIdName = name;
  }

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public String getTarget() {
    return target;
  }

  @Override
  public String getName() {
    return hashIdName;
  }
}