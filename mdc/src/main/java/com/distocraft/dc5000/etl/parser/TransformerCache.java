package com.distocraft.dc5000.etl.parser;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import ssc.rockfactory.RockFactory;

public class TransformerCache {

	private static final Logger log = Logger.getLogger("etlengine.TransformerCache");

	private static TransformerCache tfc;

	public TransformerCache() {
		tfc = this;
		log.finest("TransformerCache initialized");
	}

	public static boolean isCheckTransformations() {
		return TransformerCacheCore.isCheckTransformations();
	}

	public static void setCheckTransformations(boolean checkTransformations) {
		TransformerCacheCore.setCheckTransformations(checkTransformations);
	}

	public Transformer getTransformer(final String transformerID) {
		return TransformerCacheCore.getTransformer(transformerID);

	}

	public void revalidate(final RockFactory reprock, final RockFactory dwhrock) {
		TransformerCacheCore.revalidate(reprock, dwhrock);
	}

	/**
	 * Initialize or revalidate cache
	 */
	public void revalidate(final RockFactory reprock, final RockFactory dwhrock, final List<String> techPacks) {
		TransformerCacheCore.revalidate(reprock, dwhrock, techPacks);
	}

	/**
	 * Indicate change in a topologytable
	 */
	public void revalidateTable(final String table) {

	}

	public static TransformerCache getCache() {
		return tfc;
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
	public void updateTransformer(String tpName, final RockFactory dwhreprock, final RockFactory dwhrock)
			throws Exception {
		TransformerCacheCore.updateTransformer(tpName, dwhreprock, dwhrock);
	}

	public void testInitialize(final Map<String, Transformer> tformers) {
		TransformerCacheCore.testInitialize(tformers);
	}

}
