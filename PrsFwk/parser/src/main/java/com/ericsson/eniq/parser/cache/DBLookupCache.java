package com.ericsson.eniq.parser.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DBLookupCache {

	private static final Logger logger = LogManager.getLogger(DBLookupCache.class);
	private String dburl = null;
	private String dbusr = null;
	private String dbpwd = null;
	private Map clause_map = null;
	private Map tableToClauseMap = null;
	private static DBLookupCache dblc = null;

	public static void initialize(final String driver, final String dburl, final String dbusr, final String dbpwd) {
		dblc = new DBLookupCache();
		try {
			Class.forName(driver);
			dblc.dburl = dburl;
			dblc.dbusr = dbusr;
			dblc.dbpwd = dbpwd;

			dblc.refresh();

		} catch (Exception e) {
			logger.fatal("Fatal initialization error", e);
		}
	}

	public void add(final String sql, final boolean update) throws Exception {
		add(sql, null, update);
	}

	public void add(final String sql, final String tableName, final boolean update) throws Exception {

		clause_map.put(sql, null);

		if (tableName != null) {
			if (!tableToClauseMap.containsKey(tableName)) {
				tableToClauseMap.put(tableName, new ArrayList());
			}

			if (!((ArrayList) tableToClauseMap.get(tableName)).contains(sql)) {
				((ArrayList) tableToClauseMap.get(tableName)).add(sql);
			}
		}

		if (update) {
			update(sql);
		}
	}

	public void add(final String sql) throws Exception {
		add(sql, null);
	}

	public void add(final String sql, final String tableName) throws Exception {

		if (tableName != null) {
			if (!tableToClauseMap.containsKey(tableName)) {
				tableToClauseMap.put(tableName, new ArrayList());
			}
			if (!((ArrayList) tableToClauseMap.get(tableName)).contains(sql)) {
				((ArrayList) tableToClauseMap.get(tableName)).add(sql);
			}
		}

		if (!clause_map.containsKey(sql)) {

			clause_map.put(sql, null);
			update(sql);
		}
	}

	public Map remove(final String sql) {

		return (Map) clause_map.remove(sql);
	}

	public Map get(final String sql) {

		return (Map) clause_map.get(sql);
	}

	public static DBLookupCache getCache() {
		return dblc;
	}

	private void update(final String sql) throws Exception {

		logger.debug("Updating sql " + sql);

		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rest = null;

		try {

			if (clause_map == null) {
				clause_map = new HashMap();
			}
			con = DriverManager.getConnection(dburl, dbusr, dbpwd);

			Map rs = (Map) clause_map.get(sql);

			if (rs == null) {
				rs = new HashMap();
			}

			if (!rs.isEmpty()) {
				rs.clear();
			}
			ps = con.prepareStatement(sql);
			rest = ps.executeQuery();
			int i = 0;

			if (rest != null) {
				while (rest.next()) {
					i++;
					rs.put(rest.getString(1), rest.getString(2));
				}
			}
			clause_map.put(sql, rs);

			logger.debug("Refreshed " + i + " mapping rows for " + sql);

			logger.info("Refresh succesfully performed. " + clause_map.size() + " lookups found");

		} finally {

			if (ps != null) {
				try {
					while (ps.getMoreResults()) {
						ps.getResultSet().close();
					}
					ps.close();
				} catch (Throwable e) {
					logger.warn("Cleanup error", e);
				}
			}

			if (con != null) {
				try {
					con.close();
				} catch (Throwable e) {
					logger.warn("Cleanup error", e);
				}
			}

		}

	}

	public void refresh() throws Exception {

		refresh(null);
	}

	public void refresh(final String tableName) throws Exception {

		if (tableName == null) {
			logger.info("Refreshing all");
		} else {
			logger.info("Refreshing lookups for table " + tableName + "");
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rest = null;
		int updated = 0;

		try {

			if (tableToClauseMap == null) {
				tableToClauseMap = new HashMap();
			}
			if (clause_map == null) {
				clause_map = new HashMap();
			}
			if (clause_map.isEmpty()) {
				return;
			}

			final Iterator iter;

			if (tableName == null) {
				iter = clause_map.keySet().iterator();
			} else {
				final ArrayList list = (ArrayList) tableToClauseMap.get(tableName);
				if (list == null) {
					logger.debug("No lookups found for table " + tableName);
					return;
				}
				iter = list.iterator();
			}

			if (iter != null) {

				con = DriverManager.getConnection(dburl, dbusr, dbpwd);

				while (iter.hasNext()) {

					final String sql = (String) iter.next();

					final Map rs = new HashMap();

					ps = con.prepareStatement(sql);
					rest = ps.executeQuery();
					int i = 0;

					if (rest != null) {
						while (rest.next()) {
							i++;
							rs.put(rest.getString(1), rest.getString(2));
						}
					}
					clause_map.put(sql, rs);
					updated++;
					logger.info("Refreshed " + i + " mapping rows for " + sql);
				}
			}

			logger.info("Refresh succesfully performed. " + updated + " lookups refreshed");

		} finally {
			if (rest != null) {
				try {
					rest.close();
				} catch (Throwable e) {
					logger.warn("Cleanup error", e);
				}
			}

			if (ps != null) {
				try {
					while (ps.getMoreResults()) {
						ps.getResultSet().close();
					}
					ps.close();
				} catch (Throwable e) {
					logger.warn("Cleanup error", e);
				}
			}

			if (con != null) {
				try {
					con.close();
				} catch (Throwable e) {
					logger.warn("Cleanup error", e);
				}
			}

		}
	}

}
