package com.ericsson.eniq.parser.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;




public class DataFormatCache {

	final Map<String, DFormat> dataformats = new ConcurrentHashMap<>();
	final Map<String, DFormat> tagIdLookUpMap = new ConcurrentHashMap<>();
			
	private static class InstanceHolder {
		static final DataFormatCache dfc = new DataFormatCache();
	}
	
	private DataFormatCache() {
		
	}

	public static DataFormatCache getCache() {
		return InstanceHolder.dfc;
	}

	public void addDataFormat(String dataformatID, String tagID, String folderName,
			String transformerID) {
		DFormat dataformat;

		if (dataformats.containsKey(dataformatID)) {
			dataformat = dataformats.get(dataformatID);
			dataformat.setDataFormatID(dataformatID);
			dataformat.setFolderName(folderName);
			dataformat.setTagID(tagID);
			dataformat.setTransformerID(transformerID);
		} else {
			dataformat = new DFormat(tagID, dataformatID, folderName, transformerID);
		}

		dataformats.put(dataformatID, dataformat);
		tagIdLookUpMap.put(tagID, dataformat);
		
		//System.out.println("dataformat size : " + dataformats.size());
	}

	public void addDataItem(String dataformatID, String dataName, int colNumber, String dataID, String pi,
			String dataType, int dataSize, int dataScale, int isCounter) {

		if (dataformats.containsKey(dataformatID)) {
			DFormat dataformat = dataformats.get(dataformatID);

			DItem dItem = new DItem(dataName, colNumber, dataID, pi, dataType, dataSize, dataScale, isCounter);
			dataformat.addDItem(dItem);

			dataformats.put(dataformatID, dataformat);
		}

	}
	
	
	

	public void sortDataItems() {
		dataformats.forEach((dataFormatId,dataFormat) -> Collections.sort(dataFormat.getDitems()));
	}
	
	public DFormat getFormatByTagID(String tagId) {
		return tagIdLookUpMap.get(tagId);
	}
	
	public DFormat getDataFormat(String tagid) {
		for (Map.Entry<String, DFormat> entry : dataformats.entrySet()) {
			DFormat dataformat = entry.getValue();
			if (dataformat.getTagID().equals(tagid)) {
				return dataformat;
			}

		}
		return null;
	}

	public void listDataFormats() {
		for (Map.Entry<String, DFormat> entry : dataformats.entrySet()) {
			String id = entry.getKey();
			DFormat dataformat = entry.getValue();
			System.out.println(id + " : " + dataformat.toString());

//			for (DItem ditem : dataformat.getDitems()) {
//				System.out.println("\t" + ditem.toString());
//			}

		}
	}

}
