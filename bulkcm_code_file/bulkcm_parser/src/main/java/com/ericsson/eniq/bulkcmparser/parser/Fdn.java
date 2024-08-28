package com.ericsson.eniq.bulkcmparser.parser;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;

public class Fdn {

String rootMo = null;
	
	private Logger logger;

	/**
	 * Container for Fully Distinguished Name
	 */
	public Fdn(Logger logger) {
		
		this.logger=logger;
	}

	/**
	 * Handles a new ManagedObject
	 * 
	 * @param mo
	 *            ManagedObject name
	 * @param level
	 *            length of the FDN
	 */
	public void handle(String mo) {
		
		rootMo= mo;
	}

	/**
	 * Returns the FDN
	 * 
	 * @return String The Fully Distinguished Name
	 */
	public String getFdn() {
		
		return rootMo;
	}
	/**
	 * Returns the FDN data 
	 * 
	 * @param mo
	 * 		  String to be searched in FDN
	 * @return String - FDN data
	 * 
	 */
	public String getFdnData(String searchString)
	{	
		String fdn=getFdn();
		String fdnData="";
		try(Stream<String> stream=Stream.of(fdn.split(","));) {
			fdnData=stream.filter(s->s.contains(searchString)).collect(Collectors.joining(","));	
		}
		catch(Exception e)
		{
			logger.warn("Exception getting FDN data "+e);
		}
		return fdnData;
	}
	
	/**
	 * Returns the Element Parent
	 * 
	 * @return String - Element Parent Name
	 * 
	 */
	public String getElementParent()
	{
		String elementParentInfo="";
		String onlySub=getFdnData("SubNetwork");
		if(!onlySub.isEmpty())
		{
			elementParentInfo=onlySub.substring(onlySub.lastIndexOf('=')+1,onlySub.length());
		}
		return elementParentInfo;
	}
	
	/**
	 * Returns the Element 
	 * 
	 * @return String - Element Name
	 * 
	 */
	public String getElement()
	{
		String element="";
		String onlyMecontext=getFdnData("MeContext");
		String onlyManagedElement=getFdnData("ManagedElement");
		
		if(!onlyMecontext.isEmpty())
		{
			element=onlyMecontext.substring(onlyMecontext.lastIndexOf('=')+1,onlyMecontext.length());
		}
		else if (!onlyManagedElement.isEmpty())
		{
			element=onlyManagedElement.substring(onlyManagedElement.lastIndexOf('=')+1,onlyManagedElement.length());
		}
		
		return element;
	}
	

	/**
	 * Returns the Sender Name
	 * 
	 * @return String The Sender Name Name
	 * 
	 */

	public String getSn()
	{
		String fdn=getFdn();
		String sn;
		String onlyManagedElement=getFdnData("ManagedElement");
		if(!fdn.contains("SubNetwork")&&!fdn.contains("MeContext")) 
		{
			sn=onlyManagedElement;
		}
		else
		{
			if(!onlyManagedElement.isEmpty())
			{
				sn=fdn.substring(0,fdn.indexOf("ManagedElement")).replaceAll(",$", "");
			}
			else
			{
				sn=fdn;
			}
		}
		return sn;
		
	}
	/**
	 * Returns the Managed Object Identifier 
	 * 
	 * @return String - The Managed Object Identifier Name
	 * 
	 */
	
	public String getMoid()
	{
		String moid="";
		String fdn=getFdn();
		String onlyManagedElement=getFdnData("ManagedElement");
		String onlyMecontext=getFdnData("MeContext");
		if(!onlyManagedElement.isEmpty())
		{
			moid=fdn.substring(fdn.lastIndexOf("ManagedElement"),fdn.length());
		}
		else if(!onlyMecontext.isEmpty())
		{
			moid=onlyMecontext;
		}
		return moid;
	}
}
