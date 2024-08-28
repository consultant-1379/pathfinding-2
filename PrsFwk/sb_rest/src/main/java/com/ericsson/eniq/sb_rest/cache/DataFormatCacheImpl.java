package com.ericsson.eniq.sb_rest.cache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.ericsson.eniq.parser.cache.DataFormatCache;

public class DataFormatCacheImpl {
	
	public void readDB(String dbUrl, String username, String password, String driver, String versionid) {
		Connection con = null;
		try{
			Class.forName(driver);
			con = DriverManager.getConnection(dbUrl, username, password); 
			
			DataFormatCache cache = DataFormatCache.getCache();
		
			loadDataFormats(con, cache, versionid);
			loadDataItems(con, cache, versionid);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	public void loadDataFormats(Connection con, DataFormatCache cache, String verisonid) throws Exception {
		String sql = "select di.interfacename, im.tagid, im.dataformatid, df.foldername, im.transformerid"
				+ " from datainterface di, interfacemeasurement im, dataformat df"
				+ " where di.interfacename = im.interfacename and im.dataformatid = df.dataformatid"
				+ " and di.status = 1 and im.status = 1 and df.versionid in (select versionid from "
				+ "dwhrep.tpactivation where status = 'ACTIVE') "
				+ "and im.dataformatid like '%:mdc' ORDER BY im.dataformatid";
		try(PreparedStatement ps = con.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();) {
			while (rs.next()) {
				cache.addDataFormat(rs.getString(3), rs.getString(2), rs.getString(1), rs.getString(4), rs.getString(5));	
			}
		}
	}
	
	public void loadDataItems(Connection con, DataFormatCache cache, String verisonid) throws Exception {
		String sql = " SELECT di.dataname, di.colnumber, di.dataid, di.process_instruction, di.dataformatid, di.datatype, di.datasize, di.datascale,"
				+ " COALESCE("
				+ " (SELECT 1 FROM MeasurementCounter mc WHERE di.dataname = mc.dataname AND df.typeid = mc.typeid),"
				+ " (SELECT 1 FROM ReferenceColumn rc WHERE di.dataname = rc.dataname AND df.typeid = rc.typeid AND uniquekey = 0),"
				+ " 0) AS is_counter FROM dwhrep.dataformat df JOIN "
				+ "dwhrep.dataitem di ON df.dataformatid = di.dataformatid WHERE df.versionid in (select versionid from "
				+ "dwhrep.tpactivation where status = 'ACTIVE') "
				+ "and di.dataformatid like '%:mdc'";
		// and techpack_name = '"+ verisonid +"'
		try (PreparedStatement ps = con.prepareStatement(sql);
				ResultSet rs = ps.executeQuery();) {
			while (rs.next()) {
				cache.addDataItem(rs.getString(5), rs.getString(1), rs.getInt(2), rs.getString(3), rs.getString(4), 
						rs.getString(6), rs.getInt(7), rs.getInt(8), rs.getInt(9));	
			}
		}
		
	}
	
	
	

}
