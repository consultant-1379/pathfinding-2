package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This transformer transforms <i>source</i> field into <i>target</i>.<br>
 * It will handle source as a Timestamp and truncates it to the start time of the defined ROP period,
 * applies the predefined time zone offset and delta in minutes (positive or negative).<br>
 * <br/>
 * <br/>
 *
 * <table border="1">
 * <tr>
 *   <td>source</td>
 *   <td>REQUIRED</td>
 * </tr>
 * <tr>
 *   <td>target</td>
 *   <td>REQUIRED</td>
 * </tr>
 * </table>
 * <br />
 *
 * Parameters for this transformation are:<br />
 * <br />
 * <table border="1">
 * <tr>
 *   <td>rop</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is 15</td>
 * </tr>
 * <tr>
 *   <td>timezoneFixed</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is local time zone</td>
 * </tr>
 * <tr>
 *   <td>timezoneField</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is timezoneFixed</td>
 * </tr>
 * <tr>
 *   <td>delta</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is 0</td>
 * </tr>
 * <tr>
 *   <td>delta</td>
 *   <td>OPTIONAL</td>
 *   <td>Default is 0</td>
 * </tr>
 * </table>
 * <br />
 * @author eromsza
 */
public class ROPTime implements Transformation {

    private static final int MILLISECONDS_IN_MINUTE = 60000;

    private String src = null;

    private String source = null;

    private String target = null;

    private String name;

    private long time = 0;

    // 15 minutes ROP by default
    private int rop = 15;

    // MAX INTEGER value as offset by default
    private int offset = Integer.MAX_VALUE;

    // Timezone fixed value (if any)
    private String timezoneFixed;

    // A name of field containing Timezone (if any)
    private String timezoneField;

    // ROP Start Time delta (0 by default)
    private int delta = 0;

    ROPTime() {
    }

    @Override
    public void transform(final Map data, final Logger clog) {
        try {
            if (offset == Integer.MAX_VALUE) {
                final Calendar calendar;

                final String timezone = (timezoneField == null) ? timezoneFixed : (String) data.get(timezoneField);
                if (timezone == null) {
                    calendar = GregorianCalendar.getInstance();
                } else {
                    calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone(timezone));
                }
                this.offset = calendar.get(Calendar.ZONE_OFFSET);
            }

            // Null source - current time in milliseconds
            if ((source = (String) data.get(src)) == null) {
                time = System.currentTimeMillis();
            } else {
                try {
                    time = Timestamp.valueOf(source).getTime();
                } catch (IllegalArgumentException iae) {
                    return;
                }
            }

            // Formula to calculate ROP start/end in milliseconds or minutes starting from EPOCH localised to time zone
            data.put(
                    target,
                    String.format("%1$tY-%1$tm-%1$td %1$TT", new Timestamp((time / (rop * MILLISECONDS_IN_MINUTE))
                            * (rop * MILLISECONDS_IN_MINUTE) + delta * MILLISECONDS_IN_MINUTE + offset)));
        } catch (Exception e) {
            clog.log(Level.INFO, "ROPStartTime transformation error", e);
        }
    }

    @SuppressWarnings("hiding")
    @Override
    public void configure(final String name, final String src, final String target, final Properties props,
            final Logger clog) throws ConfigException {
        this.src = src;
        this.target = target;
        this.name = name;

        // Try to get ROP property and set if defined, default otherwise
        final String sRop = props.getProperty("rop");
        if (sRop != null) {
            this.rop = Integer.parseInt(sRop);
        }

        // Timezone as a string value
        this.timezoneFixed = props.getProperty("timezoneFixed");

        // Timezone as a value in a field
        this.timezoneField = props.getProperty("timezoneField");

        // Offset of ROP Start Time in minutes
        final String sDelta = props.getProperty("delta");
        if (sDelta != null) {
            this.delta = Integer.parseInt(sDelta);
        }
    }

    @Override
    public String getSource() throws Exception {
        return src;
    }

    @Override
    public String getTarget() throws Exception {
        return target;
    }

    @Override
    public String getName() throws Exception {
        return name;
    }
}
