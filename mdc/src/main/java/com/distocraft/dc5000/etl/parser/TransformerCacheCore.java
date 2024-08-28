package com.distocraft.dc5000.etl.parser;

import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import ssc.rockfactory.RockFactory;

public class TransformerCacheCore {

	private static final Logger log = Logger.getLogger("etlengine.TransformerCache");

	private static boolean checkTransformations = true;

	private static volatile Map<String, Transformer> transformers = new ConcurrentHashMap<>();

	private TransformerCacheCore() {

	}

	public static boolean isCheckTransformations() {
		return checkTransformations;
	}

	public static void setCheckTransformations(boolean checkTransformations) {
		TransformerCacheCore.checkTransformations = checkTransformations;
	}

	public static Transformer getTransformer(final String transformerID) {
		if (transformerID == null || transformerID.isEmpty()) {
			return null;
		}
		Transformer t = transformers.get(transformerID);
		if (t != null) {
			return t;
		} else if (isCheckTransformations()) {
			t = retryFetching(transformerID);
			if (t == null) {
				log.info("Cache miss or no such transformer \"" + transformerID + "\"");
				if (log.getLevel() == Level.FINEST) {
					printTransformers();
				}
			}
		}
		return t;
	}

	private static Transformer retryFetching(String transformerID) {
		int count = 5;
		Transformer t = null;
		while (count > 0) {
			try {
				Thread.sleep(30 * 1000);
			} catch (Exception e) {
				log.warning("Exception while retrying to refresh the cache" + e.getMessage());
			}
			count--;
			t = transformers.get(transformerID);
			if (t != null) {
				return t;
			}
		}
		return t;
	}

	private static void printTransformers() {
		log.finest("Cache status");
		for (String id : transformers.keySet()) {
			Transformer tr = transformers.get(id);
			if (tr != null) {
				log.finest("\"" + id + "\" = " + tr.toString());
			} else {
				log.finest("Transformer is null for id " + id);
			}
		}
	}

	public static void revalidate(final RockFactory reprock, final RockFactory dwhrock) {
		revalidate(reprock, dwhrock, null);
	}

	/**
	 * Initialize or revalidate cache
	 */
	public static void revalidate(final RockFactory reprock, final RockFactory dwhrock, final List<String> techPacks) {
		try {
			final Map<String, Transformer> newTransformers = new ConcurrentHashMap<>();
			final Tpactivation tpa_cond = new Tpactivation(reprock);
			tpa_cond.setStatus("ACTIVE");
			final TpactivationFactory tpaFact = new TpactivationFactory(reprock, tpa_cond);
			final Vector<Tpactivation> tps = tpaFact.get();
			for (Tpactivation tpa : tps) {
				final com.distocraft.dc5000.repository.dwhrep.Transformer t_cond = new com.distocraft.dc5000.repository.dwhrep.Transformer(
						reprock);
				t_cond.setVersionid(tpa.getVersionid());
				final com.distocraft.dc5000.repository.dwhrep.TransformerFactory tFact = new com.distocraft.dc5000.repository.dwhrep.TransformerFactory(
						reprock, t_cond);
				final Vector<com.distocraft.dc5000.repository.dwhrep.Transformer> ts = tFact.get();
				for (com.distocraft.dc5000.repository.dwhrep.Transformer t : ts) {
					final String tid = t.getTransformerid();
					newTransformers.put(tid, revalidateTransformer(tid, dwhrock, reprock));
				}
			}
			transformers = newTransformers;
		} catch (Exception e) {
			log.log(Level.WARNING, "Cache revalidation failed", e);
		}
	}

	public static Transformer revalidateTransformer(final String tid, final RockFactory dwhrock,
			final RockFactory reprock) throws Exception {

		return TransformerFactory.create(tid, dwhrock, reprock);

	}

	/**
	 * This function updates one transformer of the TransformerCache. The
	 * transformer of tech pack which name is given as parameter will be updated.
	 *
	 * @param tpName
	 *            Name of the techpack to revalidate.
	 * @param dwhreprock
	 *            RockFactory to dwhrep.
	 * @param dwhrock
	 *            RockFactory to dwh.
	 */
	public static void updateTransformer(String tpName, final RockFactory dwhreprock, final RockFactory dwhrock)
			throws Exception {
		Map<String, Transformer> newTransformers = new ConcurrentHashMap<>();
		newTransformers.putAll(transformers);
		Iterator<String> transformerIdsIter = newTransformers.keySet().iterator();
		log.fine("TransformerCache contains " + newTransformers.keySet().size() + " transformers.");
		while (transformerIdsIter.hasNext()) {
			String currTransformerId = (String) transformerIdsIter.next();
			if (currTransformerId.startsWith(tpName + ":")) {
				log.fine("Removing transformer " + currTransformerId + " from cache");
				// Remove the existing (ie. old) transformation.
				newTransformers.remove(currTransformerId);
			}
		}
		try {
			// Start adding the new transformations of this techpack.
			final Tpactivation whereTPActivation = new Tpactivation(dwhreprock);
			whereTPActivation.setStatus("ACTIVE");
			whereTPActivation.setTechpack_name(tpName);
			final TpactivationFactory tpActivationFact = new TpactivationFactory(dwhreprock, whereTPActivation);
			Vector<Tpactivation> tpActivations = tpActivationFact.get();
			for (Tpactivation tpa : tpActivations) {
				final com.distocraft.dc5000.repository.dwhrep.Transformer t_cond = new com.distocraft.dc5000.repository.dwhrep.Transformer(
						dwhreprock);
				t_cond.setVersionid(tpa.getVersionid());
				final com.distocraft.dc5000.repository.dwhrep.TransformerFactory tFact = new com.distocraft.dc5000.repository.dwhrep.TransformerFactory(
						dwhreprock, t_cond);
				final Vector<com.distocraft.dc5000.repository.dwhrep.Transformer> ts = tFact.get();
				for (com.distocraft.dc5000.repository.dwhrep.Transformer t : ts) {
					final String tid = t.getTransformerid();
					log.fine("Adding transformer " + tid + " to cache");
					newTransformers.put(tid, revalidateTransformer(tid, dwhrock, dwhreprock));
				}
			}
			// Set the updated HashMap as the class variable.
			transformers = newTransformers;
		} catch (Exception e) {
			log.severe("Updating transformer for " + tpName + " failed.");
			throw e;
		}

	}

	public static void testInitialize(final Map<String, Transformer> tformers) {
		transformers.putAll(tformers);
	}

}
