package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.parser.ParserDebugger;
import com.distocraft.dc5000.etl.parser.Transformer;

/**
 * Transformer implementation that uses transformations of xmltransformer but
 * reads configuration from dwhrep.Transformer
 * 
 * @author lemminkainen
 */
public final class DBTransformer implements Transformer {

  private final Logger log = Logger.getLogger("etlengine.DBTransformer");

  private final List transformations = new ArrayList();

  private ParserDebugger parserDebugger = null;

  private Map<String, String> allTransformationCache = new HashMap<String, String>();

  public DBTransformer(final String tID, final RockFactory rf, final RockFactory reprf) throws Exception {

    final long init_start = System.currentTimeMillis();

    log.info("Initializing transformer " + tID);

    String allTransformationID = "";

    // read the all transformationID from cache
    allTransformationID = (String) allTransformationCache.get(tID);

    if (allTransformationID == null || allTransformationID.length() == 0) {
      // Cache miss
      // ----0--- -1- ------2------- -3-
      // DC_E_RBS:b24:DC_E_RBS_PRACH:mdc
      String[] tokens = tID.split(":");
      if (tokens.length >= 4) {
        allTransformationID = tokens[0] + ":" + tokens[1] + ":ALL:" + tokens[3];
        allTransformationCache.put(tID, allTransformationID);
      } else {
        allTransformationID = "";
      }
    }

    Vector<com.distocraft.dc5000.repository.dwhrep.Transformation> trList = new Vector<com.distocraft.dc5000.repository.dwhrep.Transformation>();

    if (allTransformationID.length() > 0) {
      // get the ALL transformations
      final com.distocraft.dc5000.repository.dwhrep.Transformation tfa_cond = new com.distocraft.dc5000.repository.dwhrep.Transformation(
          reprf);
      tfa_cond.setTransformerid(allTransformationID);
      final com.distocraft.dc5000.repository.dwhrep.TransformationFactory tfaFact = new com.distocraft.dc5000.repository.dwhrep.TransformationFactory(
          reprf, tfa_cond, "ORDER BY ORDERNO");

      if (tfaFact != null && tfaFact.get() != null) {
        trList.addAll(tfaFact.get());
      }
    }

    // get the spesific transformations
    final com.distocraft.dc5000.repository.dwhrep.Transformation tf_cond = new com.distocraft.dc5000.repository.dwhrep.Transformation(
        reprf);
    tf_cond.setTransformerid(tID);
    final com.distocraft.dc5000.repository.dwhrep.TransformationFactory tfFact = new com.distocraft.dc5000.repository.dwhrep.TransformationFactory(
        reprf, tf_cond, "ORDER BY ORDERNO");

    if (tfFact != null && tfFact.get() != null) {
      trList.addAll(tfFact.get());
    }

    // Want list sorted by ORDERNO
    sortTransformations(trList);
    
    final Iterator it = trList.iterator();
    while (it.hasNext()) {
      final com.distocraft.dc5000.repository.dwhrep.Transformation t = (com.distocraft.dc5000.repository.dwhrep.Transformation) it
          .next();

      // ALL transformation

      final String tname = t.getType();
      final String src = t.getSource();
      final String tgt = t.getTarget();
      final Properties conf = new Properties();

      final String confstring = t.getConfig();

      if (confstring != null && confstring.length() > 0) {

        try {
          final InputStream bais = new ByteArrayInputStream(confstring.getBytes());
          conf.load(bais);
          bais.close();
        } catch (Exception e) {
          log.log(Level.WARNING, "Error config to properties", e);
        }
      }

      final Transformation tr = TransformationFactory.getTransformation(tname, src, tgt, conf, rf, log);

      if (tr == null) {
        log.warning("Factory failed to create transformation " + tname);
      } else {
        transformations.add(tr);
      }
    }

    log.info("Transformer initialized. " + transformations.size() + " transformations configured.");
    log.fine("Transformer initialization took " + (System.currentTimeMillis() - init_start) + " ms");

  }

  public void transform(final Map data, final Logger log) throws Exception {

    final long trans_start = System.currentTimeMillis();

    final Iterator id = transformations.iterator();
    while (id.hasNext()) {
      final Transformation t = (Transformation) id.next();
      if (parserDebugger != null) {
        parserDebugger.beforeTransformation(data);
      }

      t.transform(data, log);

      if (parserDebugger != null) {
        parserDebugger.afterTransformation(t.getName(), data);
      }
    }

    log.finest("Transformation performed in " + (System.currentTimeMillis() - trans_start) + " ms");

  }

  public void addDebugger(ParserDebugger parserDebugger) {
    // TODO Auto-generated method stub
    this.parserDebugger = parserDebugger;
  }

  public List getTransformations() {
    return transformations;
  }
  
  private void sortTransformations(Vector list) {
		Collections.sort(list, new Comparator() {

			public int compare(Object o1, Object o2) {
				com.distocraft.dc5000.repository.dwhrep.Transformation c1 = (com.distocraft.dc5000.repository.dwhrep.Transformation) o1;
				com.distocraft.dc5000.repository.dwhrep.Transformation c2 = (com.distocraft.dc5000.repository.dwhrep.Transformation) o2;

				return c1.getOrderno().compareTo(c2.getOrderno());
			}

			public boolean equals(Object o1, Object o2) {
				com.distocraft.dc5000.repository.dwhrep.Transformation c1 = (com.distocraft.dc5000.repository.dwhrep.Transformation) o1;
				com.distocraft.dc5000.repository.dwhrep.Transformation c2 = (com.distocraft.dc5000.repository.dwhrep.Transformation) o2;

				return c1.getOrderno().equals(c2.getOrderno());
			}
		});
	}

}
