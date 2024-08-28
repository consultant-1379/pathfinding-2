package com.ericsson.eniq.loadfilebuilder.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TableNameCache {

	@Value("${db.repdb.url}")
	private  String dbUrl;

	@Value("${db.repdb.dwhrep.user}")
	private  String user;

	@Value("${db.repdb.dwhrep.pass}")
	private  String pass;

	@Value("${versionid.list}")
	private  String versionIdString;

	private static String dataFormatIdPattern = "^(.+):.+:(.+):.+$";

	private static Map<String, String> tagIdToTpMap = new ConcurrentHashMap<>();

	private static Map<String, String> tagIdToTableNameMap = new ConcurrentHashMap<>();

	private static String sql = "SELECT TAGID, DATAFORMATID FROM DefaultTags WHERE DATAFORMATID LIKE ?  or DATAFORMATID LIKE ?";

	private static final Logger LOG = LogManager.getLogger(TableNameCache.class);

	
	public void initCache() throws SQLException {
		String[] versionIds = versionIdString.split(",");
		DriverManager.registerDriver(new com.sybase.jdbc4.jdbc.SybDriver());
		ResultSet resultSet = null;
		try (Connection conn = DriverManager.getConnection(dbUrl, user, pass);
				PreparedStatement statement = conn.prepareStatement(sql);
				) {
			statement.setString(1, "%"+versionIds[0]+"%");
			statement.setString(2, "%"+versionIds[1]+"%");
			resultSet = statement.executeQuery();
			String tagId;
			String dataFormatId;
			Matcher m;
			while (resultSet.next()) {
				tagId = resultSet.getString("TAGID");
				dataFormatId = resultSet.getString("DATAFORMATID");
				m = extractFolder(dataFormatId);
				tagIdToTpMap.put(tagId, m.group(1));
				tagIdToTableNameMap.put(tagId, m.group(2));
			}
		}
		finally {
			if (resultSet != null) {
				resultSet.close();
			}
		}
		LOG.log(Level.INFO, "Initialized tagIdToTpMap : " + tagIdToTpMap);
		LOG.log(Level.INFO, "Initialized tagIdToTableNameMap: " + tagIdToTableNameMap);
	}

	private static Matcher extractFolder(String dataFormatId) {
		Matcher m;
		Pattern p = Pattern.compile(dataFormatIdPattern);
		m = p.matcher(dataFormatId);
		if (m.matches()) {
			return m;
		}
		return m;
	}

	public static void main(String[] args) {
		try {
			TableNameCache cache = new TableNameCache();
			cache.initTestValues();
			String[] tags = {"BatteryBackup","EUtranCellFDD","EUtranCellFDD_FLEX","RiPort_V"};
			cache.initCache();
			LOG.log(Level.INFO, "tagIdToTpMap Size : " + tagIdToTpMap.size());
			LOG.log(Level.INFO, "tagIdToTableNameMap Size : " + tagIdToTableNameMap.size());
			for (String tagId : tags) {
				LOG.log(Level.INFO, " TP for : "+tagId +" = "+getTpName(tagId));
				LOG.log(Level.INFO, " Table Name for : "+tagId +" = "+getFolderName(tagId));
			}
			
		} catch (SQLException e) {
			LOG.log(Level.WARN, "Exception while initializing cache", e);
		}
	}
	
	private void initTestValues() {
		dbUrl = "jdbc:sybase:Tds:10.36.255.71:2641";
		user = "dwhrep";
		pass = "dwhrep";
	}

	public static String getFolderName(String tagId) {
		return tagIdToTableNameMap.get(tagId);
	}

	public static String getTpName(String tagId) {
		return tagIdToTpMap.get(tagId);
	}

}
