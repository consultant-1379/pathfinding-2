package com.ericsson.eniq.parser.cache;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DFormat {
  
   private String tagID;
  private String dataFormatID;
  private String folderName;
  private String transformerID;
  
  private List<DItem> ditems = new ArrayList<DItem>();
  
  public DFormat(final  String tid, final String dfid, final String fname, final String trID) {
    tagID = tid;
    dataFormatID = dfid;
    folderName = fname;
    transformerID = trID;
  }

  public String getDataFormatID() {
    return dataFormatID;
  }
  
  public void setDataFormatID(String dataformatID) {
	  this.dataFormatID = dataformatID;
  }

  public List<DItem> getDitems() {
    return ditems;
  }

  public String getFolderName() {
    return folderName;
  }
  
  public void setFolderName(String folderName) {
	  this.folderName = folderName;
  }
  

  public String getTransformerID() {
    return transformerID;
  }
  
  public void setTransformerID(String transformerID) {
	  this.transformerID = transformerID;
  }

  
 
  public String getTagID() {
    return tagID;
  }
  
  public void setTagID(String tagID) {
	  this.tagID = tagID;
  }
  
  public int getDItemCount() {
    return ditems.size();
  }
  
  public void setItems(List<DItem> list) {
    ditems = list;
  }
  
  public void addDItem(DItem ditem) {
	  ditems.add(ditem);
  }
  
  
  @Override
public String toString() {
	return "DFormat [tagID=" + tagID + ", dataFormatID=" + dataFormatID
			+ ", folderName=" + folderName + ", transformerID=" + transformerID + "]";
}

public List<DItem> getItems() {
	  return ditems;
  }
  
}

