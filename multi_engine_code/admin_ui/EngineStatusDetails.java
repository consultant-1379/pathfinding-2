package com.distocraft.dc5000.etl.gui.systemmonitor;

import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.velocity.Template;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

import com.distocraft.dc5000.common.RmiUrlFactory;
import com.distocraft.dc5000.etl.engine.main.ITransferEngineRMI;
import com.distocraft.dc5000.etl.gui.common.EtlguiServlet;

/**
 * Copyright &copy; Ericsson ltd. All rights reserved.<br>
 * This server shows details of the engine status.<br>
 * @author Janne Berggren
 */
public class EngineStatusDetails extends EtlguiServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5716708271510359431L;

	private Log log = LogFactory.getLog(this.getClass());

	public Template doHandleRequest(HttpServletRequest request, HttpServletResponse response, Context ctx) {
		Template page = null;
		final String usePage = "engine_status_details.vm";

		ctx.put("engineStatusDetails", getEngineStatusDetails());
		// finally generate page
		try {
			page = getTemplate(usePage);
		} catch (ResourceNotFoundException e) {
			log.error("ResourceNotException", e);
		} catch (ParseErrorException e) {
			log.error("ParseErrorException", e);
		} catch (Exception e) {
			log.error("Exception", e);
		}

		// and return with the template
		return page;
	}
	
	public static int getConsolidatedStatus(int currentStatus, int previousStatus) {
		if (previousStatus == MonitorInformation.BULB_GRAY) {
			return currentStatus;
		}
		if (previousStatus == MonitorInformation.BULB_YELLOW || currentStatus == MonitorInformation.BULB_YELLOW) {
			return MonitorInformation.BULB_YELLOW;
		}
		if (previousStatus == MonitorInformation.BULB_GREEN && currentStatus == MonitorInformation.BULB_GREEN) {
			return MonitorInformation.BULB_GREEN;
		}
		if (previousStatus == MonitorInformation.BULB_RED && currentStatus == MonitorInformation.BULB_RED) {
			return MonitorInformation.BULB_RED;
		}else {
			return MonitorInformation.BULB_YELLOW;
		}
	}
	
	public static String getCurrentProfileName(final String currProfile) {
	    try {
	      final String[] splitted = currProfile.split(":");
	      return splitted[1].trim();
	    } catch (final Exception e) {
	      return "";
	    }
	  }

	public MonitorInformation getEngineStatusDetails() {
		ITransferEngineRMI transferEngineRmi;
		MonitorInformation monitorInformation = new MonitorInformation();
		try {
			
			List<String> engineUrls = RmiUrlFactory.getInstance().getAllEngineRmiUrls();
			log.info("Engine Urls :"+engineUrls);
			int prevStatus = MonitorInformation.BULB_GRAY;
			int currentStatus = MonitorInformation.BULB_GRAY;
			monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
			for (String engineUrl : engineUrls) {
				try {
					transferEngineRmi = (ITransferEngineRMI) Naming.lookup(engineUrl);
					List<String> status = transferEngineRmi.status();

					//monitorInformation.setCurrProfile((status.get(13)));
					if ("NoLoads".equalsIgnoreCase(getCurrentProfileName(status.get(13)))) {
						currentStatus = MonitorInformation.BULB_YELLOW;
					} else if ((status.get(7)).trim().equals(MonitorInformation.STATUS_ENGINE_STRING_OK)) {
						currentStatus = MonitorInformation.BULB_GREEN;
					} else {
						currentStatus = MonitorInformation.BULB_YELLOW;
					}
					prevStatus = getConsolidatedStatus(currentStatus, prevStatus);
					//monitorInformation.setUptime(((String) status.get(4)));
					//monitorInformation.setTotMem(
							//"Total Memory: " + MonitorInformation.transformBytesToMegas((String) status.get(17)) + " Mb.");
					//monitorInformation.setSize("Priority queue " + ((String) status.get(8)).toLowerCase());
					
					/*log.info("MonitorInformation status   :"+monitorInformation.getStatus());
					log.info("MonitorInformation Uptime   :"+monitorInformation.getUptime());
					log.info("MonitorInformation Total Mem   :"+monitorInformation.getTotMem());
					log.info("MonitorInformation size   :"+monitorInformation.getSize());*/

					final Iterator<String> iterator = status.iterator();

					while (iterator.hasNext()) {
						monitorInformation.setMessage((iterator.next()) + "<br />");
					}
					
				} catch (Exception e) {
					currentStatus = MonitorInformation.BULB_RED;
					prevStatus = getConsolidatedStatus(currentStatus, prevStatus);
					log.error("Exception ", e);
				}
	
			}
			log.info("MonitorInformation  message :"+monitorInformation.getMessage());
			if (monitorInformation.getMessage() != null && !monitorInformation.getMessage().isEmpty() ) {
				monitorInformation.setStatus(prevStatus);
			} else {
				monitorInformation.setStatus(MonitorInformation.BULB_RED);
				monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
				log.error("Exception while getting engine urls");
			}
		} catch (Exception e) {
			monitorInformation.setStatus(MonitorInformation.BULB_RED);
			monitorInformation.setFieldName(MonitorInformation.TITLE_ETLENGINE);
			log.error("Exception while getting engine urls", e);
			
		}
		

		return monitorInformation;

	}

}
