package com.ericsson.eniq.sbkafkabulkcm.cache;

import java.sql.SQLException;

import com.distocraft.dc5000.repository.dwhrep.Tpactivation;
import com.distocraft.dc5000.repository.dwhrep.TpactivationFactory;
import com.distocraft.dc5000.repository.dwhrep.Transformation;
import com.distocraft.dc5000.repository.dwhrep.TransformationFactory;
import com.distocraft.dc5000.repository.dwhrep.Transformer;
import com.distocraft.dc5000.repository.dwhrep.TransformerFactory;
import com.ericsson.eniq.parser.cache.TransformerCache;

import ssc.rockfactory.RockException;
import ssc.rockfactory.RockFactory;

public class TransformerCacheImpl {

	public void readDB(String DBurl, String username, String password, String driver, String DBname, String VersionID) {

		try {
			RockFactory dwhRepRock = new RockFactory(DBurl, username, password, driver, DBname, false);

			final Tpactivation tpa_cond = new Tpactivation(dwhRepRock);
			tpa_cond.setStatus("ACTIVE");
			// tpa_cond.setTechpack_name(VersionID);
			final TpactivationFactory tpaFact = new TpactivationFactory(dwhRepRock, tpa_cond);
			for (Tpactivation tpa : tpaFact.get()) {
				Transformer t_cond = new Transformer(dwhRepRock);
				t_cond.setVersionid(tpa.getVersionid());
				TransformerFactory t_fac = new TransformerFactory(dwhRepRock, t_cond);
				for (Transformer transformer : t_fac.get()) {
					if (transformer.getTransformerid().endsWith(":bcd")) {
//						System.out.println(transformer.getTransformerid());
						Transformation tran_cond = new Transformation(dwhRepRock);
						tran_cond.setTransformerid(transformer.getTransformerid());
						TransformationFactory tranFac = new TransformationFactory(dwhRepRock, tran_cond,
								"ORDER BY ORDERNO");
						for (Transformation transformation : tranFac.get()) {

							TransformerCache.getCache().addTransformation(transformation.getTransformerid(),
									transformation.getType(), transformation.getSource(), transformation.getTarget(),
									transformation.getConfig());

						}
					}
				}
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RockException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
