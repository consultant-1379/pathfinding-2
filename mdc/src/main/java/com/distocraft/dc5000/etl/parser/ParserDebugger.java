package com.distocraft.dc5000.etl.parser;

import java.util.List;
import java.util.Map;
import java.util.Vector;

public interface ParserDebugger {

  void started();
  void newMeasurementFile(MeasurementFile mf);
  void removeMeasurementFile(MeasurementFile mf);
  void finished();
  void failed(Exception e);

  void beforeTransformer(String transformerid, Map map);
  void afterTransformer(String transformerid, Map map);
  void result(Map map);
  
  void beforeTransformation(Map data);
  void afterTransformation(String name, Map data);
  
  void setDatatitems(String transformerid,List dis);
  
}
