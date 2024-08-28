package com.ericsson.eniq.sbkafka.cache;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import com.ericsson.eniq.parser.cache.DItem;
import com.ericsson.eniq.parser.cache.DataFormatCache;
import com.ericsson.eniq.sbkafka.controller.pojo.InterfaceProperties;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

@Component
public class DataFormatCacheImpl {

	private static Map<String, String> dataFormatIdToTagIdMap = new HashMap<>();
	private static Map<String, List<DItem>> dataFormatIdToDItemsMap = new HashMap<>();
	private static Map<String, Schema> schemaRegistry = new ConcurrentHashMap<>();
	
	public static String bigString;
	public static String smallString;
	private static InterfaceProperties props;
	
	private static Set<String> tagIds = new HashSet<>();

	private static final Logger LOG = LogManager.getLogger(DataFormatCacheImpl.class);
	
	@Value("${schema.registry.url}") 
	private String schemaRegistryUrl;
	
	@Value("${producer.topic}") 
	private String producerTopic;
	
	@Value ("${parser.type}")
	private String parserType;
	
	@Value("${tp.list}")
	private String tpList;
	
	@Value("classpath:/intf.json")
	Resource interfacePropertiesFile;
	
	private String inClauseValue;
	
	
	

	public void readDB(String dbUrl, String username, String password, String driver) {
		Connection con = null;
		try {
			loadInterfaceProperties();
			Class.forName(driver);
			con = DriverManager.getConnection(dbUrl, username, password);

			DataFormatCache cache = DataFormatCache.getCache();
			extractInClauseValue();
			loadDataFormats(con, cache);
			loadDataItems(con, cache);
			LOG.info("Data formatcache initialized successfully ");
			getSchemas();
			//generateTestData();

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
	
	private void extractInClauseValue() {
		if (tpList != null && !tpList.trim().isEmpty()) {
			StringBuilder inClauseValueBuilder = new StringBuilder();
			for (String s : tpList.trim().split(",")) {
				inClauseValueBuilder.append("'");
				inClauseValueBuilder.append(s);
				inClauseValueBuilder.append("'");
				inClauseValueBuilder.append(",");
			}
			inClauseValue = inClauseValueBuilder.toString();
			
			if (inClauseValue.endsWith(",")) {
				inClauseValue = inClauseValue.substring(0,inClauseValue.length()-1);
			}
			LOG.log(Level.INFO,"inclause value = " + inClauseValue);
		} else {
			LOG.log(Level.WARN,"no valid inforamtion provided in tp.list");
		}
	}

	public void loadDataFormats(Connection con, DataFormatCache cache) throws Exception {
				
		String sql = "select im.tagid, im.dataformatid, df.foldername, im.transformerid"
				+ " from datainterface di, interfacemeasurement im, dataformat df"
				+ " where di.interfacename = im.interfacename and im.dataformatid = df.dataformatid"
				+ " and di.status = 1 and im.status = 1 and df.versionid in (select versionid from "
				+ "dwhrep.tpactivation where status = 'ACTIVE' and TECHPACK_NAME IN ("+inClauseValue+"))"
				+ "and im.dataformatid like '%:"+parserType+"' ORDER BY im.dataformatid";
		int rowcount = 0;
		try (PreparedStatement ps = con.prepareStatement(sql); ResultSet rs = ps.executeQuery();) {

			while (rs.next()) {
				cache.addDataFormat(rs.getString(2), rs.getString(1), rs.getString(3),
						rs.getString(4));
				dataFormatIdToTagIdMap.put(rs.getString(2), rs.getString(1));
				tagIds.add(rs.getString(1));
				rowcount++;
			}

		}
		LOG.info("total rows read = " + rowcount);

	}

	public void loadDataItems(Connection con, DataFormatCache cache) throws Exception {
		String sql = " SELECT di.dataname, di.colnumber, di.dataid, di.process_instruction, di.dataformatid, di.datatype, di.datasize, di.datascale,"
				+ " COALESCE("
				+ " (SELECT 1 FROM MeasurementCounter mc WHERE di.dataname = mc.dataname AND df.typeid = mc.typeid),"
				+ " (SELECT 1 FROM ReferenceColumn rc WHERE di.dataname = rc.dataname AND df.typeid = rc.typeid AND uniquekey = 0),"
				+ " 0) AS is_counter FROM dwhrep.dataformat df JOIN "
				+ "dwhrep.dataitem di ON df.dataformatid = di.dataformatid WHERE df.versionid in (select versionid from "
				+ "dwhrep.tpactivation where status = 'ACTIVE' and TECHPACK_NAME IN ("+inClauseValue+")) " + "and di.dataformatid like '%:mdc'";
		// and techpack_name = '"+ verisonid +"'
		try (PreparedStatement ps = con.prepareStatement(sql); ResultSet rs = ps.executeQuery();) {
			String dataFormatId;
			String dataName;
			int colNumber;
			String dataID;
			String pi;
			String dataType;
			int dataSize;
			int dataScale;
			int isCounter;
			while (rs.next()) {
				dataFormatId = rs.getString(5);
				dataName = rs.getString(1);
				colNumber = rs.getInt(2);
				dataID = rs.getString(3);
				pi = rs.getString(4);
				dataType = rs.getString(6);
				dataSize = rs.getInt(7);
				dataScale = rs.getInt(8);
				isCounter = rs.getInt(9);
				cache.addDataItem(dataFormatId, dataName, colNumber, dataID, pi, dataType, dataSize, dataScale,
						isCounter);
				List<DItem> dItems = dataFormatIdToDItemsMap.get(dataFormatId);
				if (dItems == null) {
					dItems = new ArrayList<>();
					dataFormatIdToDItemsMap.put(dataFormatId, dItems);
				}
				dItems.add(new DItem(dataName, colNumber, dataID, pi, dataType, dataSize, dataScale, isCounter));
			}

		}
		LOG.info("Sorting data Items ");
		cache.sortDataItems();
		LOG.info("data Items sorted");
		//initSchemaRegistry();
		//getSchemas();
	}

	private void initSchemaRegistry() {
		String tagId;
		List<DItem> dItems;
		LOG.log(Level.INFO, "dataFormatIdToDItemsMap : " + dataFormatIdToDItemsMap);
		for (Map.Entry<String, List<DItem>> entry : dataFormatIdToDItemsMap.entrySet()) {
			dItems = entry.getValue();
			Collections.sort(dItems);
			tagId = dataFormatIdToTagIdMap.get(entry.getKey());
			if (tagId != null) {
				schemaRegistry.put(tagId, SchemaGenerator.getAvroSchema(tagId, dItems.iterator()));
			} else {
				LOG.info("initSchemaRegistry: Folder name is null : " + entry.getKey());
			}

		}

	}

	private  void getSchemas() {
		for (String tagId : tagIds) {
			String subjectName = producerTopic + "-" + tagId;
			WebClient webClient = WebClient.builder().baseUrl(schemaRegistryUrl)
					.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).build();
			Subject subject = webClient.method(HttpMethod.GET).uri("/subjects/" + subjectName + "/versions/latest")
					.exchange().block().bodyToMono(Subject.class).block();
			String schemaString = subject.getSchema();
			Schema.Parser parser = new Schema.Parser();
			if (schemaString != null) {
				Schema schema = parser.parse(schemaString);
				schemaRegistry.put(tagId, schema);
				LOG.log(Level.INFO, "Schema for tagId : "+tagId+" is :"+schema);
			} else {
				LOG.info("Schema is not present for tagId : " + tagId);
			}
		
		}
		LOG.info("Schema registry initialized , size : "+ schemaRegistry.size());

	}

	public static Schema getSchema(String folderName) {
		return schemaRegistry.get(folderName);
	}
	
	private static void generateTestData(){
		Set<String> combinations = combination("ABCDEFGHIJ");
		Map<String, String> testMap = new HashMap<>();
		for (String s : combinations) {
			testMap.put(s, s);
		}
		bigString = testMap.toString();
		smallString = combinations.toString();
		LOG.log(Level.INFO, "bigString Size ="+ bigString.getBytes().length);
		LOG.log(Level.INFO, "smallString Size ="+ smallString.getBytes().length);
	}
	
	private static Set<String> combination(String input)	{
		Set<String> set = new HashSet<>();
		if (input.length() == 1) {
			set.add(input);
		} else {
			for (int i = 0; i < input.length(); i++) {
				String pre = input.substring(0, i);
				String post = input.substring(i + 1);
				String remaining = pre + post;
				if (set.size() >= 100) {
					break;
				}
				
				for (String permutation : combination(remaining)) {
					set.add(input.charAt(i) + permutation);
					if (set.size() >= 100) {
						break;
					}
					
				}
			}
		}
		return set;
	}
	
	private void loadInterfaceProperties() {
		final Type type = new TypeToken<InterfaceProperties>() {
		}.getType();
		Gson gson = new Gson();
		try(InputStreamReader isr = new InputStreamReader(interfacePropertiesFile.getInputStream())) {
			JsonReader reader = new JsonReader(isr);
			props = gson.fromJson(reader, type);
		} catch (IOException e) {
			LOG.log(Level.WARN, "not able to load the interface properties :",e);
		}
		LOG.log(Level.INFO, " loaded interface properties = "+props.getActionContents());
	}
	
	public static Map<String, String> getActionContents() {
		LOG.log(Level.INFO, "loaded action contents = "+props.getActionContents());
		return Collections.unmodifiableMap(props.getActionContents());
	}
	
	/*public static void main(String[] args) {
		WebClient webClient = WebClient.builder().baseUrl("http://10.45.193.129:32015")
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE).build();
		Subject subject = webClient.method(HttpMethod.GET).uri("/subjects/PM_E_ERBS_DATA-" + "CapacityConnectedUsers_V" + "/versions/latest")
				.exchange().block().bodyToMono(Subject.class).block();
		//Gson gson = new GsonBuilder().serializeNulls().create();
		//Subject sub = gson.fromJson(subject, Subject.class);
		String schemaString = subject.getSchema();
		Schema.Parser parser = new Schema.Parser();
		Schema avrSchema = parser.parse(schemaString);
		System.out.println("avrschema : "+avrSchema);
		//AvrSchema schema = subject.getSchema();
	}*/

}
