package com.ericsson.eniq.common;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import com.distocraft.dc5000.common.ConsoleLogFormatter;
import com.distocraft.dc5000.common.ServicenamesHelper;

public class EngineLogger extends Handler {

  private static final DateFormat form = new SimpleDateFormat("yyyy_MM_dd");

  protected String logdir;

  private final Map<String,OutputDetails> logs = new HashMap<String,OutputDetails>();

  private boolean deb = false;

  public EngineLogger() throws IOException, SecurityException {

    final String plogdir = System.getProperty("LOG_DIR");
    if (plogdir == null) {
      throw new IOException("System property \"LOG_DIR\" not defined");
    }
      
    logdir = plogdir + File.separator + "engine";
    
    setLevel(Level.ALL);
    setFormatter(new ConsoleLogFormatter());

    final String xdeb = System.getProperty("EngineLogger.debug");
    if (xdeb != null && xdeb.length() > 0) {
      deb = true;
    }
    
  }

  /**
   * Does nothing because publish will handle flush after writing
   */
  public synchronized void flush() {

    if (deb) {
      System.err.println("EL.flush()");
    }
      
  }

  public synchronized void close() {

    if (deb) {
      System.err.println("EL.close()");
    }
      
    final Iterator<String> i = logs.keySet().iterator();

    while (i.hasNext()) {

      try {

        final String key = i.next();

        final OutputDetails od = logs.get(key);
        od.out.close();
        od.out = null;
        
        od.outPriorityQue.close();
        od.outPriorityQue = null;

        i.remove();

      } catch (Exception e) {
        //
      }

    }

  }

  /**
   * Publish a LogRecord
   */
  public synchronized void publish(final LogRecord record) {

    if (deb) {
      System.err.println("EL.publish(" + record.getLoggerName() + ")");
    }
      
    // Determine that level is loggable and filter passes
    if (!isLoggable(record)) {
      return;
    }

    String ipAddress;
    String type = "engine";
    try {
		ipAddress = InetAddress.getLocalHost().getHostAddress();
		type = ServicenamesHelper.getServiceName("Engine", ipAddress, null);
	} catch (UnknownHostException e1) {
		e1.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
    String tp = "NA";
    

    final String logname = record.getLoggerName();
    
    try {

      //final String logname = record.getLoggerName();

      // Special handling for these loggers
      if (logname.startsWith("etl.")) {
        final int ix = logname.indexOf(".") + 1;
        tp = logname.substring(ix, logname.indexOf(".", ix));
      } else if (logname.startsWith("sql.")) {
        final int ix = logname.indexOf(".") + 1;
        tp = logname.substring(ix, logname.indexOf(".", ix));
        type = type+"_sql";
      } else if (logname.startsWith("file.")) {
        final int ix = logname.indexOf(".") + 1;
        tp = logname.substring(ix, logname.indexOf(".", ix));
        type = type+"_file";
      } else if (logname.startsWith("sqlerror.")) {
        final int ix = logname.indexOf(".") + 1;
        tp = logname.substring(ix, logname.indexOf(".", ix));
        type = type+"_sqlerror";
      } else if (logname.startsWith("aggregator.")) {
          tp = "NA";
          type = type+"_aggregator";
      } else if (logname.equals("lwphelper")) {
        tp = logname;
        type = logname;
      }

    } catch (Exception e) {
      if(deb) {
        e.printStackTrace();
      }
    }

    if (deb) {
      System.err.println("EL: TechPackName is \"" + tp + "\" type is \"" + type + "\"");
    }
      
    OutputDetails od = logs.get(tp + "_" + type);

    final Date dat = new Date(record.getMillis());

    final String dstamp = form.format(dat);

    if (od == null || !dstamp.equals(od.dat)) {
      od = rotate(tp, type, dstamp);
    }

    try {
      //od.out.write(getFormatter().format(record));
      //od.out.flush();
    	
    	if(logname.indexOf("PriorityQueue")>=0)
        {
      	  od.outPriorityQue.write(getFormatter().format(record));	
      	  od.outPriorityQue.flush();
        }else {
      	  od.out.write(getFormatter().format(record));
            od.out.flush();
        }

      if (deb) {
        System.err.println("Written: " + record.getMessage());
      }
    } 
	catch (InterruptedIOException e){}
	catch (Exception ex) {
      ex.printStackTrace();
    }
    
    final int levelInt = record.getLevel().intValue();
    if (levelInt >= Level.WARNING.intValue()) {
      if(deb) {
        System.err.println("EL: Logging error");
      }
      
      OutputDetails odw = logs.get("WARN_error");
      
    if (odw == null || !dstamp.equals(odw.dat)) {
        odw = rotate("WARN", "error", dstamp);
      }
        
      try {
        odw.out.write(getFormatter().format(record));
        odw.out.flush();

        if (deb) {
          System.err.println("EL: Written to error: " + record.getMessage());
        }
      } 
	  catch (InterruptedIOException e){}
	  catch (Exception ex) {
        ex.printStackTrace();
      }
      
    }

  }

  private OutputDetails rotate(final String tp, final String type, final String timestamp) {

    if (deb) {
      System.err.println("EL.rotate(" + tp + " " + type + " " + timestamp + ")");
    }
      
    OutputDetails od = null;

    try {

      od = logs.get(tp + "_" + type);

      if (od == null) {
        od = new OutputDetails();
      } else if (od.out != null) { // a file is already open
        od.out.close();
      }
      
      if(od.outPriorityQue != null){
    	  od.outPriorityQue.close();
      }

      final String dirx;

      if ("NA".equals(tp)) {
        dirx = logdir + File.separator;
      } else if ("WARN".equals(tp)) {
        dirx = logdir + File.separator;
      } else {
        dirx = logdir + File.separator + tp;
      }	
      
      final File dir = new File(dirx);
      if (!dir.exists() && !dir.mkdirs()) {
        System.err.println("EL: LogRotation failed to create directory " + dir.getPath());
        return od;
      }

      final File f = new File(dir, type + "-" + timestamp + ".log");
      
      final File fprio;

      if (deb) {
        System.err.println("EL: FileName is " + f.getCanonicalPath());
      }
      //int size = 100;  
      //od.out = new FileWriter(f, true);
      od.out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(f, true)));
      od.dat = timestamp;

      od.out.write(getFormatter().getHead(this));

      logs.put(tp + "_" + type, od);
      
      if ("NA".equals(tp) && (!type.equals("aggregator"))) {
    	  fprio = new File(dir, type + "-PriorityQueue" + "-" + timestamp + ".log");
    	  
    	  od.outPriorityQue = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(fprio, true)));
          od.dat = timestamp;

          od.outPriorityQue.write(getFormatter().getHead(this));

      }

    } catch (Exception e) {
      System.err.println("EL: LogRotation failed");
      e.printStackTrace();
    }

    return od;

  }

  public class OutputDetails {

    public Writer out = null;
    public Writer outPriorityQue = null;

    public String dat = null;

  }

}
