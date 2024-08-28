package com.distocraft.dc5000.etl.parser;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class TransformationDebug {

  private String[] targets;
  private String[] source;
  private Vector data;
  private String name;
  private boolean first;

  private String transformerId;

  public TransformationDebug() {
    
    source  = null;
    data = new Vector();
    targets = null;
    name = "";
    first = true;
  }

  public void before(Map firstdata) {
    
    if (!first){
      return;
    }
    
    Map tmp = new HashMap();
    Iterator iter = firstdata.keySet().iterator();
    while (iter.hasNext()) {
      String key = (String) iter.next();
      //System.out.println(name+":  "+key);
      tmp.put(key, firstdata.get(key));
    }

     data.add(tmp); 
     first = false;
  }

  public void after(Map newData) {

    Map tmp = new HashMap();
    Iterator iter = newData.keySet().iterator();
    while (iter.hasNext()) {
      String key = (String) iter.next();
      //System.out.println(name+":  "+key);
      tmp.put(key, newData.get(key));
    }

     data.add(tmp);  
   }
  
  public void setTransformerId(String transformerId) {
    this.transformerId = transformerId;
  }
  
  public String getTransformerId() {
    return this.transformerId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Vector getData() {
    return data;
  }
  
}
