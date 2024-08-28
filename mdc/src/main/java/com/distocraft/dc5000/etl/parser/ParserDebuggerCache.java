package com.distocraft.dc5000.etl.parser;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.distocraft.dc5000.repository.cache.DItem;

public class ParserDebuggerCache implements ParserDebugger {

  private static ParserDebuggerCache pdc = null;

  private Map data;

  private Map dataitems;

  private String TransformerId;

  TransformationDebug transformation;

  public static void initialize() {
    pdc = new ParserDebuggerCache();
    pdc.data = new HashMap();
    pdc.dataitems = new HashMap();
  }

  public static ParserDebuggerCache getCache() {
    return pdc;
  }

  public void afterTransformer(String transformerid, Map map) {
    // TODO Auto-generated method stub
    //System.out.println("afterTransformer "+transformerid);

    if (pdc.data.containsKey(transformerid)) {
      ((List) pdc.data.get(transformerid)).add(transformation.getData());
    } else {
      List l = new Vector();
      l.add(transformation.getData());
      pdc.data.put(transformerid, l);
    }
  }

  public void beforeTransformer(String transformerid, Map map) {
    // TODO Auto-generated method stub
    //System.out.println("beforeTransformer "+transformerid);
    transformation = new TransformationDebug();

  }

  public void failed(Exception e) {
    // TODO Auto-generated method stub
    //System.out.println("failed " + e);
  }

  public void finished() {
    // TODO Auto-generated method stub
    //System.out.println("finished");
  }

  public void newMeasurementFile(MeasurementFile mf) {
    // TODO Auto-generated method stub
    //System.out.println("newMeasurementFile");
  }

  @Override
  public void removeMeasurementFile(MeasurementFile mf) {
    // TODO Auto-generated method stub
    // //System.out.println("newMeasurementFile");
  }

  public void result(Map map) {
    // TODO Auto-generated method stub
    //System.out.println("result");
  }

  public void started() {
    // TODO Auto-generated method stub
    //System.out.println("started");
  }

  public void beforeTransformation(Map data) {
    // TODO Auto-generated method stub
    //System.out.println("beforeTransformation");
    transformation.before(data);
  }

  public void afterTransformation(String name, Map data) {
    // TODO Auto-generated method stub
    //System.out.println("afterTransformation");
    transformation.setName(name);
    transformation.after(data);
  }

  public Map getData() {
    return data;
  }

  public Vector<String> getDataitems(String transformerid) {
    return (Vector<String>) dataitems.get(transformerid);
  }

  public void setDatatitems(String transformerid, List dis) {
    //System.out.println("setDatatitems");

    try {

      if (!dataitems.containsKey(transformerid)) {

        Iterator iterator = dis.iterator();
        while (iterator.hasNext()) {
          final DItem mData = (DItem) iterator.next();

          if (dataitems.containsKey(transformerid)) {
            ((Vector<String>) dataitems.get(transformerid)).add(mData.getDataID());
            //System.out.println(mData.getDataID());
          } else {
            Vector<String> v = new Vector<String>();
            v.add(mData.getDataID());
            //System.out.println(mData.getDataID());
            dataitems.put(transformerid, v);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public String getTransformerId() {
    return TransformerId;
  }

  public void setTransformerId(String transformerId) {
    TransformerId = transformerId;
  }
}
