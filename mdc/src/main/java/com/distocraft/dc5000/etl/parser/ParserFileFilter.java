package com.distocraft.dc5000.etl.parser;

import java.io.File;
import java.io.FileFilter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO intro <br> 
 * TODO usage <br>
 * TODO used databases/tables <br>
 * TODO used properties <br>
 * <br>
 * Copyright Distocraft 2005<br>
 * <br>
 * $id$
 * For CR 102310918-FCP 103 8147 Filename Filter
 * New ParserFileFilter constructor and method acceptByFileName is implemented
 *
 * @author lemminkainen
 */
public class ParserFileFilter implements FileFilter {

    private int fileCountLimit;

    public int getFileCountLimit() {
		return fileCountLimit;
	}

	public void setFileCountLimit(int fileCountLimit) {
		this.fileCountLimit = fileCountLimit;
	}

	int filesAccepted = 0;

    int filesRejected = 0;

    int checkType = 0;
    
    //CR 102310918-FCP 103 8147 Filename Filter
    // Defined new variables for File name filtering and logging 
    private Pattern fileNameFilter = null;
    
    private final Logger log;
    
    // CR 102310918-FCP 103 8147 Filename Filter 
    //Keep this constructor for first release so if someone forget to take changes of Main.class
    // Functionality will not break

    public ParserFileFilter(final int fileCountLimit, final int checkType) {
    	this.log = Logger.getLogger("etl.parser.ParserFileFilter");
        this.fileCountLimit = fileCountLimit;
        this.checkType = checkType;
    }
    
    /**
     * New constructor is defined with new parameter to support filtering file
     * @param fileCountLimit
     * @param checkType
     * @param fileNameFilter
     */
     public ParserFileFilter(final int fileCountLimit, final int checkType, final String fileNameFilter){
       this.log = Logger.getLogger("etl.parser.ParserFileFilter");
       this.fileCountLimit = fileCountLimit;
       this.checkType = checkType;
       
       if(null != fileNameFilter && !"".equals(fileNameFilter)){
         try{
           this.fileNameFilter = Pattern.compile(fileNameFilter);
         }
         catch(Exception e){
        	 
        	 e.printStackTrace();
        	 this.log.log(Level.INFO, "Filter:" + fileNameFilter+ "is invalid so reading all files by default");
        	 this.fileNameFilter = Pattern.compile("");
           
         }
       }
     
   }

    /**
     * @see java.io.FileFilter#accept(java.io.File)
     */
    public boolean accept(final File file) {

        if (filesAccepted > fileCountLimit) {
            filesRejected++;
            return false;
        }
        
        // CR 102310918-FCP 103 8147 Filename Filter
        //following condition is used to check if file name is valid as per configuration defined by TP 
        if(!acceptByFileName(file.getName())){
          
          this.log.log(Level.FINEST, "File:" + file.getName()+ "is invalid file as per configuration");
         filesRejected++;
         return false;
       }

        if (checkType == 0) {
            filesAccepted++;
        } else if (checkType == 1) {
            if (file.exists()) {
                filesAccepted++;
            } else {
                filesRejected++;
            }
        } else if (checkType == 2) {
            if (file.isFile()) {
                filesAccepted++;
            } else {
                filesRejected++;
            }
        } else if (checkType == 3) {
            if (file.canRead()) {
                filesAccepted++;
            } else {
                filesRejected++;
            }
        } else {
            filesAccepted++;
        }

        return true;

    }
    
    /**
     *  This method is used to accept files in In directory based on configuration
     *  provided by TP 
     * @param fileName
     * @return
     */
    
    private boolean acceptByFileName(final String fileName){
      
      boolean returnValue = true;
      
      // check only if filename filter is configured
      
      if(null!=fileNameFilter){
        // do not break functionality as this feature is for Ericsson Internal use only
        
        try{
          final Matcher mat = fileNameFilter.matcher(fileName);
          returnValue = mat.matches();
        }
        catch(Exception e){
          
        }
      }
      
      return returnValue;
    }

}
