/**
 * ----------------------------------------------------------------------- *
 * Copyright (C) 2011 LM Ericsson Limited. All rights reserved. *
 * -----------------------------------------------------------------------
 */
package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

//import com.distocraft.dc5000.etl.engine.main.engineadmincommands.InvalidArgumentsException;

/**
 * Used to convert IP address (v4 or v6) based on the source into binary data type for database. Binary data type for the database is a hexadecimal
 * value.
 * 
 * @author epaujor
 * 
 */
public class ConvertIpAddress implements Transformation {

    private static final String THREE_COLON_STR = ":::";

    private static final String TWO_COLON_STR = "::";

    private static final String ZERO = "0";

    private static final int NO_OF_IP_V6_PARTS = 8;

    private static final int NO_OF_IP_V4_PARTS = 4;

    private static final String START_OF_TARGET = "0x";

    private static final String COLON_SEPARATOR = ":";

    private static final String DOT_SEPARATOR = ".";

    // Need the escape characters with "."
    private static final String DOT_SEPARATOR_WITH_ESCAPE = "\\.";

    private String source = null;

    private String target = null;

    private String convertIpAddressName;

    @Override
    public void transform(final Map data, final Logger clog) throws Exception, IOException {
        final String input = (String) data.get(source);

        String convertedIpAddress;
        if (input != null) {
            if (input.contains(DOT_SEPARATOR)) {
                convertedIpAddress = convertIpAddr(input, NO_OF_IP_V4_PARTS, DOT_SEPARATOR_WITH_ESCAPE);
            } else if (input.contains(COLON_SEPARATOR)) {
                convertedIpAddress = convertIpAddr(input, NO_OF_IP_V6_PARTS, COLON_SEPARATOR);
            } else {
                throw new Exception(input + " is not a valid IPv4 or IPv6 IP address");
            }

            data.put(target, convertedIpAddress);

        }
    }

    /**
     * Converts the IPv4 or IPv6 IP address into a hexadecimal value which will be stored in the binary data type.
     * 
     * For example, if IPv4=10.10.10.17, then converted value will be 0x0a0a0a11 hexadecimal.
     * 
     * For example, if IPv6=2001:0db8:85a3:0:0000:8a2e:0370:7334, then converted value will be 0x20010db885a3000000008a2e03707334 hexadecimal.
     * 
     * "0x" needs to be added to the start when inserting into the database
     * 
     * @param data
     * @param input
     * @param numOfIpAddrParts
     * @param separator
     * @throws InvalidArgumentsException
     */
    private String convertIpAddr(final String input, final int numOfIpAddrParts, final String separator) throws Exception {
        final String[] partsOfIpAddr = input.split(separator);
        final StringBuilder convertedIpAddress = new StringBuilder();

        if (partsOfIpAddr.length == numOfIpAddrParts) {
            // "0x" needs to be added when inserting into the database
            convertedIpAddress.append(START_OF_TARGET);
            for (String partOfIpAddr : partsOfIpAddr) {
                convertedIpAddress.append(convertToHexString(partOfIpAddr, numOfIpAddrParts));
            }
        } else if (separator.equals(COLON_SEPARATOR) && partsOfIpAddr.length < numOfIpAddrParts) {
            if (input.endsWith(TWO_COLON_STR)) {
                // 2001:7334:: is valid. Need to handle valid case where 2001:7334:: is
                // equivalent to 2001:7334:0:0:0:0:0:0. Need to append 0 in order for this 
                // case to work, otherwise, there will be an infinite loop
                final String inputStr = input.concat("0");
                convertedIpAddress.append(convertIpAddr(inputStr, numOfIpAddrParts, separator));
            } else if (input.contains(TWO_COLON_STR)) {
                // 2001::7334 is valid. Need to handle valid case where 2001::7334 is
                // equivalent to 2001:0:0:0:0:0:0:7334
                final int numberofMissingParts = numOfIpAddrParts - partsOfIpAddr.length;
                String inputStr = input;
                for (int i = 0; i < numberofMissingParts; i++) {
                    inputStr = input.replaceFirst(TWO_COLON_STR, THREE_COLON_STR);
                }
                convertedIpAddress.append(convertIpAddr(inputStr, numOfIpAddrParts, separator));
            } else {
                throw new Exception(input + " is not a valid IPv4 or IPv6 IP address");
            }

        } else {
            throw new Exception(input + " is not a valid IPv4 or IPv6 IP address");
        }
        return convertedIpAddress.toString();
    }

    private String convertToHexString(final String partOfIpAddr, final int numOfIpAddrParts) {
        String convertedHexString;
        if (numOfIpAddrParts == NO_OF_IP_V4_PARTS) {
            // For IPv4, need to convert integer parts to hex
            convertedHexString = Integer.toHexString(Integer.parseInt(partOfIpAddr));
            convertedHexString = addLeadingZeros(convertedHexString, 2);
        } else {
            // Otherwise assume IPv6. IPv6 parts are already in hex
            convertedHexString = partOfIpAddr;
            convertedHexString = addLeadingZeros(convertedHexString, 4);
        }
        return convertedHexString;
    }

    /**
     * Each part of the hexadecimal string must be two characters long for IPv4. Each part of the hexadecimal string must be four characters long for
     * IPv6.
     * 
     * @param convertedHexString
     * @param expectNoOfDigits
     * @return
     */
    private String addLeadingZeros(final String convertedHexString, final int expectNoOfDigits) {
        final StringBuffer hexStrWithZeros = new StringBuffer();
        if (convertedHexString.length() < expectNoOfDigits) {
            final int noOfLeadingZeros = expectNoOfDigits - convertedHexString.length();
            for (int i = 0; i < noOfLeadingZeros; i++) {
                hexStrWithZeros.append(ZERO);
            }
        }
        hexStrWithZeros.append(convertedHexString);
        return hexStrWithZeros.toString();
    }

    @Override
    public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog) {
        this.source = src;
        this.target = tgt;
        this.convertIpAddressName = name;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getTarget() {
        return target;
    }

    @Override
    public String getName() {
        return convertIpAddressName;
    }
}
