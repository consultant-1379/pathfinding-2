package com.distocraft.dc5000.etl.gui.monitor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.logging.Level;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.commons.lang.StringEscapeUtils;

import ssc.rockfactory.RockFactory;

import com.distocraft.dc5000.etl.gui.common.CalSelect;
import com.distocraft.dc5000.etl.gui.common.DbCalendar;
import com.distocraft.dc5000.etl.gui.common.EtlguiServlet;

/**
 * Copyright &copy; Distocraft ltd. All rights reserved. <br>
 * Servlet for showing loaded measurement types. <br>
 * dc5000dwh database is used.
 * 
 * VM: showLoadings.vm
 * VM: cal_select_1.vm
 * 
 * @author Perti Raatikainen
 */
public class ShowLoadings extends EtlguiServlet {

  private static final long serialVersionUID = 1L;
  
  private final transient Log log = LogFactory.getLog(this.getClass());

  //private static final SimpleDateFormat sdf_secs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /**
   * @see org.apache.velocity.servlet.VelocityServlet#handleRequest(javax.servlet.http.HttpServletRequest,
   *      javax.servlet.http.HttpServletResponse,
   *      org.apache.velocity.context.Context)
   */

  public Template doHandleRequest(final HttpServletRequest request, final HttpServletResponse response, final Context ctx) {

    Template outty = null;

    // use this template
    final String page = "showloadings.vm";

    // connections to database
    final RockFactory rockDwhRep = (RockFactory) ctx.get("rockDwhRep");
    final RockFactory rockDwh = (RockFactory) ctx.get("rockDwh");

    final HttpSession session = request.getSession(false);

    // get the current date (is used at the UI, if none given)
    final DbCalendar calendar = new DbCalendar();

    // check if user has given any parameters
    // day parameters
    String year_1 = request.getParameter("year_1");
    String month_1 = request.getParameter("month_1");
    String day_1 = request.getParameter("day_1");

    String techPackName = request.getParameter("techPackName");
    final String problematic = StringEscapeUtils.escapeHtml(request.getParameter("problematic"));

    final String getInfo = StringEscapeUtils.escapeHtml(request.getParameter("getInfoButton"));

    String latestUpdate = "unknown";

    String pattern =  "^[a-zA-Z0-9_-]*$";
    
    if(techPackName == null){
    	techPackName = "-";
    }
    if(techPackName.matches(pattern)){
    	techPackName = StringEscapeUtils.escapeHtml(techPackName);
    }else{
    	techPackName = null;
    }
    
    String pattern2 = "^[0-9]*$";
    
    if(year_1 == null){
    	year_1 = "-";
    }
    
    if(year_1.matches(pattern2)){
    	year_1 = StringEscapeUtils.escapeHtml(year_1);
    }else{
    	year_1 = null;
    }
    
    if(month_1 == null){
    	month_1 = "-";
    }
    
    
    if(month_1.matches(pattern2)){
    	month_1 = StringEscapeUtils.escapeHtml(month_1);
    }else{
    	month_1 = null;
    }
    
    if(day_1 == null){
    	day_1 = "-";
    }
    
    
    if(day_1.matches(pattern2)){
    	day_1 = StringEscapeUtils.escapeHtml(day_1);
    }else{
    	day_1 = null;
    }
    
    //This sends a vector of valid years from DIM_DATE Table.
    //This is used by cal_select_1.vm
    final CalSelect calSelect = new CalSelect(rockDwh.getConnection());
    ctx.put("validYearRange", calSelect.getYearRange());
    
    try {

      // year session info
      if (year_1 != null) {
        session.setAttribute("year", year_1);
      } else if (session.getAttribute("year") != null) {
        year_1 = session.getAttribute("year").toString();
      } else {
        session.setAttribute("year", calendar.getYearString());
        year_1 = calendar.getYearString();
      }

      // month session info
      if (month_1 != null) {
        session.setAttribute("month", month_1);
      } else if (session.getAttribute("month") != null) {
        month_1 = session.getAttribute("month").toString();
      } else {
        session.setAttribute("month", calendar.getMonthString());
        month_1 = calendar.getMonthString();
      }

      // day session info
      if (day_1 != null) {
        session.setAttribute("day", day_1);
      } else if (session.getAttribute("day") != null) {
        day_1 = session.getAttribute("day").toString();
      } else {
        session.setAttribute("day", calendar.getDayString());
        day_1 = calendar.getDayString();
      }

      // techpack name session info
      if (techPackName != null) {
        session.setAttribute("tpName", techPackName);
      } else if (session.getAttribute("tpName") != null) {
        techPackName = session.getAttribute("tpName").toString();
      } else {
        session.setAttribute("tpName", "-");
        techPackName = "-";
      }

      // *end* session info

      ctx.put("problematic", problematic == null ? "" : "checked");
      ctx.put("selectedTechPack", techPackName);

      final List<String> tps = Util.getActiveNonEventsTechPacks(rockDwhRep.getConnection());
      ctx.put("distinctTechPacks", tps);

      log.debug("techPackName: " + techPackName);
      log.debug("button: " + getInfo);

      Map<Integer, String> showHours = new TreeMap<Integer, String>();

      for (int i = 0; i < 24; i++) {
        showHours.put(new Integer(i), "Y");
      }

      if (getInfo != null) {

        log.debug("Getting load statuses...");

        final String date = year_1 + "-" + month_1 + "-" + day_1;

        // get all status
        final List<List<String>> measurementTypes = getMeasurementTypes(date, techPackName, rockDwh.getConnection(), rockDwhRep.getConnection());

        int measurementTypeslen = measurementTypes.size();
        
        if( measurementTypes != null &&  measurementTypeslen != 0) {

        // join measurement type and check if there is any problems
        final List<LoadingDetails> loadStatuses = joinMeatypes(rockDwh.getConnection(), measurementTypes, problematic);

        // get latest modifed time from log_loadstatus table
        latestUpdate = getLatestTimestamp(rockDwh.getConnection());

        log.debug("End getting load statuses...");

        for (int i = 0; i < loadStatuses.size(); i++) {
          log.debug("loadStatuses[" + i + "] = " + loadStatuses.get(i));
        }

      	ctx.put("loadStatuses", loadStatuses); 
      	
      	final Vector<LoadingDetails> problematicLoadStatus = new Vector<LoadingDetails>();
       
        if (problematic != null && problematic.equalsIgnoreCase("problematic")) {          
          showHours = new TreeMap<Integer, String>();
          for (int i = 0; i < 24; i++) {
            showHours.put(new Integer(i), "N");
          }
          // Check what hours should be shown when problematic filter is on.
          boolean allMeasurementTypesLoaded = true;
          for (int i = 0; i < loadStatuses.size(); i++) {
            final LoadingDetails currLoadingStatus = (LoadingDetails) loadStatuses.get(i);

            final List<String> hourStatuses = currLoadingStatus.getStatuses();

            for (int j = 0; j < hourStatuses.size(); j++) {
              final String currHourStatus = hourStatuses.get(j);

              if (!currHourStatus.equalsIgnoreCase("LOADED") && !currHourStatus.equalsIgnoreCase("RESTORED")) {
                // There is some problematic loading at this hour. The hour must
                // be drawn.
                showHours.put(new Integer(j), "Y");
                allMeasurementTypesLoaded = false;
              }
            }
            if(!allMeasurementTypesLoaded){
            	problematicLoadStatus.add(currLoadingStatus);
            	allMeasurementTypesLoaded = true;
            }
          }
          ctx.remove("loadStatuses");
          // Show only the measurement types which have problematic loading.
          if(problematicLoadStatus.size() > 0){
        	  ctx.put("loadStatuses", problematicLoadStatus);
          }
          else{
            final String emptyString = "";            	
        	  ctx.put("loadStatuses", emptyString);
          }
        }       
      }      
      }

      ctx.put("showHours", showHours);
      ctx.put("year_1", year_1);
      ctx.put("month_1", month_1);
      ctx.put("day_1", day_1);
      ctx.put("latestUpdate", latestUpdate);

      outty = getTemplate(page);
    } catch (ResourceNotFoundException e) {
      log.debug("ResourceNotFoundException (getTemplate): ", e);
    } catch (ParseErrorException e) {
      log.debug("ParseErrorException (getTemplate): ", e);
    } catch (Exception e) {
      log.debug("Exception (getTemplate): ", e);
    }
    return outty;
  }

  /**
   * get latest timestamp from log load status table
   * 
   * @return timestamp string
   */

  private String getLatestTimestamp(final Connection conn) {

    String ts = "unknown";

    final String sql = "SELECT DATEFORMAT(max(modified),'yyyy-mm-dd hh:nn:ss') as modified FROM log_loadstatus";

    Statement stmt = null;
    ResultSet rset = null;

    try {
      // make status query to the monitoring tables
      stmt = conn.createStatement();
      log.debug("Executing query: " + sql);
      rset = stmt.executeQuery(sql);

      if (rset != null) {
        rset.next();
        /*if (rset.getTimestamp(1) != null) {
          ts = sdf_secs.format(rset.getTimestamp(1));
        }*/
        final String maxModified = rset.getString(1);
        if (maxModified != null) {
          ts = maxModified;
        }
      }
    } catch (SQLException e) {

    } finally {
      try {
        if (rset != null) {
          rset.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        log.error("SQLException", e);
      }
    }
    return ts;
  }

  /**
   * Joins meatypes which have same name, timelevel and same hour
   * 
   * @param measurementTypes
   * @return
   */

  private List<LoadingDetails> joinMeatypes(final Connection conn, final List<List<String>> measurementTypes, final String problematic) {

    final Vector<LoadingDetails> mt = new Vector<LoadingDetails>();

    String meatype = null;
    String timelevel = "";
    String hour = "";

    int rowCounter = 0;
    
    LoadingDetails ld = null;

    if (measurementTypes.size() == 0) {
      ld = new LoadingDetails();
      ld.addStatus("");
    } else {
      for (final Iterator<List<String>> i = measurementTypes.iterator(); i.hasNext();) { // kaikki mtypet

        final List<String> mtype = i.next();

        for (final Iterator<String> t = mtype.iterator(); t.hasNext();) { // Values

          final String meatypeName = t.next();
          final String tlevel = t.next();
          final String status = t.next();
          final String statusHour = t.next();

          if (meatype == null) {
            ld = new LoadingDetails();
            ld.setMeatypeName(meatypeName);
            ld.setTimelevel(tlevel);
            ld.addStatus(status);
            
            if (status.equalsIgnoreCase("LOADED") || status.equalsIgnoreCase("RESTORED")) {
              ld.setProblematic(false);
            } else {
              ld.setProblematic(true);
            }
            //System.out.println("Joining MT:" + meatypeName); // meaType name
            //System.out.println("Initial status hour:" +status);
            //System.out.println("Timelevel:" + tlevel); // timelevel
            //System.out.println("Adding"+(ld.getProblematic() ? " problematic":"")+" MT:" + ld.getMeatypeName()); // meaType name
            //System.out.println("problematic: " + (problematic != null ? problematic:"") + " meatype problematic: " + ld.getProblematic());

          } else if (meatypeName.equalsIgnoreCase(meatype) && tlevel.equalsIgnoreCase(timelevel)) {

            if (hour.equalsIgnoreCase(statusHour)) {
              // If the hour is the same as the previous hour:
              ld = updateLDStatusInSameHour(ld, tlevel, status);
            } else {
              log.debug("Status:" +status);
              ld.addStatus(status);
              //System.out.println("Hour: "+statusHour);
              //System.out.println("Status: "+status);
              if (status.equalsIgnoreCase("LOADED") || status.equalsIgnoreCase("RESTORED")) {
                ld.setProblematic(false);
              } else {
                ld.setProblematic(true);
              }
            } 
          } else {

            log.debug("Joining MT:" + meatypeName); // meaType name
            log.debug("Timelevel:" + tlevel); // timelevel
            //System.out.println("Joining MT:" + meatypeName); // meaType name
            //System.out.println("Timelevel:" + tlevel); // timelevel

            ld.setRowCount(rowCounter);

            log.debug("Adding"+(ld.getProblematic() ? " problematic":"")+" MT:" + ld.getMeatypeName()); // meaType name
            log.debug("problematic: " + (problematic != null ? problematic:"") + " meatype problematic: " + ld.getProblematic());
            //System.out.println("Adding"+(ld.getProblematic() ? " problematic":"")+" MT:" + ld.getMeatypeName()); // meaType name
            //System.out.println("problematic: " + (problematic != null ? problematic:"") + " meatype problematic: " + ld.getProblematic());

            ld.setDuration(getDurationminutes(timelevel, conn));
            mt.add(ld);

            ld = new LoadingDetails();
            rowCounter = 0;

            ld.setMeatypeName(meatypeName);
            ld.setTimelevel(tlevel);
            ld.addStatus(status);

            if (status.equalsIgnoreCase("LOADED") || status.equalsIgnoreCase("RESTORED")) {
              ld.setProblematic(false);
            } else {
              ld.setProblematic(true);
            }
          }
          meatype = meatypeName;
          timelevel = tlevel;
          hour = statusHour;
          rowCounter++;
        }
      }
    }

    ld.setRowCount(rowCounter);
    log.debug("Adding"+(ld.getProblematic() ? " problematic":"")+" MT:" + ld.getMeatypeName()); // meaType name
    log.debug("problematic: " + (problematic != null ? problematic:"") + " meatype problematic: " + ld.getProblematic());
    ld.setDuration(getDurationminutes(timelevel, conn));
    mt.add(ld);

    // mt.remove(0);
    return mt;
  }

  /**
   * Update LoadingDetails status within the same hour.
   * 
   * @param loadingDetails
   *          The LoadingDetails, with data from the previous row in the
   *          database.
   * @param tlevel
   *          The time level from Log_LoadStatus for the current row. Can be
   *          15MIN, HOUR, 24HOUR etc.
   * @param status
   *          The status read from Log_LoadStatus for the current row.
   * @return ld Updated LoadingDetails object is returned.
   */
  protected LoadingDetails updateLDStatusInSameHour(final LoadingDetails loadingDetails, final String tlevel,
      final String status) {
    if (loadingDetails.getProblematic()) {
      // Previous row has an error. Keep status as it is.
      // Except when the time level is at HOUR, take current row status instead
      // (loaded):
      if ((status.equalsIgnoreCase("LOADED") || status.equalsIgnoreCase("RESTORED"))
              && tlevel.equalsIgnoreCase("HOUR")
              && (loadingDetails.getStatus().equalsIgnoreCase("HOLE") || loadingDetails.getStatus().equalsIgnoreCase(
                  "NOT_LOADED"))) {
        loadingDetails.updateStatus(status);
        loadingDetails.setProblematic(false);
      }
    } else if (!status.equalsIgnoreCase("LOADED") && !status.equalsIgnoreCase("RESTORED")) {
      // Previous row ok. Current row has error, so update the status to the
      // error status.
      // Except when time level is at HOUR, take previous row status instead (loaded):
      if ((status.equalsIgnoreCase("NOT_LOADED") || status.equalsIgnoreCase("HOLE")) && tlevel.equalsIgnoreCase("HOUR")
              && (loadingDetails.getStatus().equalsIgnoreCase("LOADED") || loadingDetails.getStatus().equalsIgnoreCase("RESTORED"))) {
        loadingDetails.updateStatus(loadingDetails.getStatus());
        loadingDetails.setProblematic(false);
      } else {
        loadingDetails.updateStatus(status);
        loadingDetails.setProblematic(true);
      }
    } else {
      log.debug("Previous row is not problematic and current row is LOADED, status will not be changed");
    }
    return loadingDetails;
  }

  private int getDurationminutes(final String timelevel, final Connection conn) {

    int duration = 0;

    final String sql = "SELECT durationmin FROM dim_timelevel WHERE timelevel = '" + timelevel + "'";

    Statement stmt = null;
    ResultSet rset = null;

    try {
      // make status query to the monitoring tables
      stmt = conn.createStatement();
      log.debug("Executing query: " + sql);
      rset = stmt.executeQuery(sql);

      if (rset != null) {
        while (rset.next()) {
          duration = rset.getInt(1);
        }
      }
    } catch (SQLException e) {

    } finally {
      try {
        if (rset != null) {
          rset.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        log.error("SQLException", e);
      }
    }
    return duration;
  }

  /**
   * Get active measurement types
   * 
   * @param typeName
   * @param connDRep
   * @return String "AND typename IN ('MEATYPE1','MEATYPE2')"
   */

  private String getActiveMeasurementTypes(final String typeName, final Connection connDRep) {

    String meaTypeList = "AND typename IN (";

    List<String> ll = new ArrayList<String>();

    // if tech pack is selected get all tech packs active meatypes
    // else get all measurement types which have active meatypes

    if (typeName != null && !typeName.equals("") && typeName.length() > 1) {
      ll = Util.getMeasurementTypes(typeName, connDRep);
    } else {
      ll = Util.getAllMeasurementTypes(connDRep);
    }

    final int listLength = ll.size();

    if (listLength > 0) {

      final ListIterator<String> li = ll.listIterator();

      for (int i = 0; i < ll.size(); i++) {
        meaTypeList += "'" + li.next() + "'";
        meaTypeList += ",";
      }
      meaTypeList = meaTypeList.substring(0, meaTypeList.length() - 1);
      meaTypeList += ")";
    } else {
      meaTypeList = "";
    }


    return meaTypeList;

  }

  /**
   * Get typename, timelevel, status, datatime form log_loadstatus
   * 
   * @param date
   * @param techpackName
   * @param conn
   * @param connDRep
   * @return
   */

  private List<List<String>> getMeasurementTypes(final String date, final String techpackName, final Connection conn, final Connection connDRep) {

    final List<List<String>> result = new ArrayList<List<String>>();

    log.info("########Type :" + techpackName);

    final String meaTypeList = getActiveMeasurementTypes(techpackName, connDRep);
    
    final String beginTime = date + " 00:00:00";
    final String endTime = date + " 23:59:59";
    
    if (meaTypeList != null && !meaTypeList.equals("") && meaTypeList.length() > 0) {

    final String sql = "SELECT typename, timelevel, status, datatime FROM log_loadstatus WHERE "
      + "datatime BETWEEN '" + beginTime + "' AND '" + endTime + "' " 
      + meaTypeList + " ORDER BY typename, timelevel, datatime";

    Statement stmt = null;
    ResultSet rset = null;

    try {
      
      // make status query to the monitoring tables
      stmt = conn.createStatement();
      //String tempOptionSQL = "SET TEMPORARY OPTION RETURN_DATE_TIME_AS_STRING = 'ON';";
      //log.debug("sql: " + tempOptionSQL);
      //stmt.executeQuery(sql);
      //stmt.executeQuery(tempOptionSQL);
      
      log.debug("Executing query: " + sql);
      rset = stmt.executeQuery(sql);

      //tempOptionSQL = "SET TEMPORARY OPTION RETURN_DATE_TIME_AS_STRING = 'OFF';";
      //log.debug("sql: " + tempOptionSQL);
      //stmt.executeQuery(tempOptionSQL);
      
      while (rset.next()) {
        final List<String> vt = new ArrayList<String>();
        vt.add(rset.getString(1));
        vt.add(rset.getString(2));
        vt.add(rset.getString(3));
        vt.add(rset.getString(4).substring(11, 13));
        if ("DC_E_ERBS_EUTRANCELLTDD".equalsIgnoreCase(rset.getString(1))) {
        	log.info("EUTRANCELLFDD results : " + vt);
        }
        result.add(vt);
      }

    } catch (SQLException e) {

    } finally {
      try {
        if (rset != null) {
          rset.close();
        }
        if (stmt != null) {
          stmt.close();
        }
      } catch (SQLException e) {
        log.error("SQLException", e);
      }
    }
    }

    return result;
  }

}