                                               package com.distocraft.dc5000.etl.parser.xmltransformer;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This transformer class can be used for calculations. Input
 * for this transformer is <i>formula</i>. <i>formula</i> defines the
 * calculation formula, for example "20/" means that number 20 is divided with
 * input and "-20" means that input is reduced by 20. If calculation fails the
 * data map remains unchanged.<br /> <br />
 * <table border="1">
 * <tr>
 * <td>type</td>
 * <td>calculation</td>
 * </tr>
 * <tr>
 * <td>source</td>
 * <td>REQUIRED</td>
 * </tr>
 * <tr>
 * <td>target</td>
 * <td>REQUIRED</td>
 * </tr>
 * <tr>
 * </table>
 * <br />
 * 
 * This transformation can be performed two ways. Fixed formula can be defined
 * or variable can be used. Either formula or varformula parameter must be
 * defined.<br /> <br />
 * <table border="1">
 * <tr>
 * <td>formula</td>
 * <td>OPTIONAL</td>
 * <td>Used calculation formula. For example -20 or /60</td>
 * </tr>
 * <tr>
 * <td>varformula</td>
 * <td>OPTIONAL</td>
 * <td>Used calculation formula with variable name. For example -VAR_NAME or
 * VAR_NAME/</td>
 * </tr>
 * <tr>
 * <td>usefloat</td>
 * <td>OPTIONAL</td>
 * <td>If this parameter is set to true input field and operand are parsed as
 * floating point numbers.</td>
 * </tr>
 * </table>
 * 
 * 
 * 
 * Copyright by Ericsson 2005-7 <br>
 * All rights reserved
 * 
 * @author Timo Melkko
 * @author etuolem
 */
public class Calculation implements Transformation {

  private String src;

  private String tgt;

  private String name;

  private long fixedInt = 0;

  private double fixedFloat = 0;

  private String operandName = null;

  private int operation = 0;

  private boolean useFloat = false;

  public void transform(final Map data, final Logger log) {

    final String tinput = (String) data.get(src);

    if (tinput == null) {
      return;
    }

    if (useFloat) {

      double input = 0;

      try {
        input = Double.parseDouble(tinput);
      } catch (NumberFormatException nfe) {
        log.finer("CalculationTransformation: Source " + tinput + " is not a float");
        return;
      }

      double operand = fixedFloat;

      if (operandName != null) {
        final String toperand = (String) data.get(operandName);

        if (toperand == null) {
          return;
        }

        try {
          operand = Double.parseDouble(toperand);
        } catch (NumberFormatException nfe) {
          log.finer("CalculationTransformation: Operand " + toperand + " is not a float");
          return;
        }
      }

      double result = 0;

      try {
        switch (operation) {
        case 30:
          log.warning("For Calculation of PMRes Quantity Measurment, double precision should not " +
          		"be used (float option for transformation in tech pack should be set to false). " +
          		"Result will be invalid.");
        case 11:
          result = input / operand;
          break;
        case 12:
          result = input * operand;
          break;
        case 13:
          result = input + operand;
          break;
        case 14:
          result = input - operand;
          break;
        case 21:
          result = operand / input;
          break;
        case 22:
          result = operand * input;
          break;
        case 23:
          result = operand + input;
          break;
        case 24:
          result = operand - input;
          break;
        }
        data.put(tgt, String.valueOf(result));

      } catch (Exception e) {
        log.log(Level.FINE, "Calculation error", e);
      }
      
    } else {

      long input = 0L;

      try {
        input = Long.parseLong(tinput);
      } catch (NumberFormatException nfe) {
        log.finer("CalculationTransformation: Source " + tinput + " is not an integer");
        return;
      }

      long operand = fixedInt;

      if (operandName != null) {
        final String toperand = (String) data.get(operandName);

        if (toperand == null) {
          return;
        }

        try {
          operand = Long.parseLong(toperand);
        } catch (NumberFormatException nfe) {
          log.finer("CalculationTransformation: Operand " + toperand + " is not an integer");
          return;
        }
      }

      long result = 0L;

      try {
        switch (operation) {
        case 30: //For Calculation of Quantity Measurement of a PMRes counter.
          double tmp = (double) input;
          result = (new Double(Math.IEEEremainder((tmp / 256), 1)*256)).longValue();
          break;
        case 11:
          result = input / operand;
          break;
        case 12:
          result = input * operand;
          break;
        case 13:
          result = input + operand;
          break;
        case 14:
          result = input - operand;
          break;
        case 21:
          result = operand / input;
          break;
        case 22:
          result = operand * input;
          break;
        case 23:
          result = operand + input;
          break;
        case 24:
          result = operand - input;
          break;
        }
        data.put(tgt, String.valueOf(result));

      } catch (Exception e) {
        log.log(Level.FINE, "Calculation error", e);
      }

    }

  }

  public void configure(final String name, final String src, final String tgt, final Properties props, final Logger clog)
      throws ConfigException {
    
    this.name = name;
    this.src = src;
    this.tgt = tgt;

    try {

      if ("true".equals(props.getProperty("usefloat"))) {
        useFloat = true;
      }

      final String formula = props.getProperty("formula");
      final String varformula = props.getProperty("varformula");

      char operator = 0;

      if (formula != null && formula.length() >= 2) {

        // Parameter formula is used

        try {
        	
          if (formula.equalsIgnoreCase("pmresquantity")){
        	  operation = 30;
          } else if ((formula.charAt(0) == '/') || (formula.charAt(0) == '*') || (formula.charAt(0) == '+')
              || (formula.charAt(0) == '-')) {
            operation = 10;
            operator = formula.charAt(0);
            if (useFloat) {
              fixedFloat = Double.parseDouble(formula.substring(1));
            } else {
              fixedInt = Long.parseLong(formula.substring(1));
            }

          } else if ((formula.charAt(formula.length() - 1) == '/') || (formula.charAt(formula.length() - 1) == '*')
              || (formula.charAt(formula.length() - 1) == '+') || (formula.charAt(formula.length() - 1) == '-')) {
            operation = 20;
            operator = formula.charAt(formula.length() - 1);
            if (useFloat) {
              fixedFloat = Double.parseDouble(formula.substring(0, formula.length() - 1));
            } else {
              fixedInt = Long.parseLong(formula.substring(0, formula.length() - 1));
            }

          } else {
            throw new ConfigException("Parameter formula is mallformed: \"" + formula + "\"");
          }

        } catch (NumberFormatException nfe) {
          throw new ConfigException("Parameter formula is invalid. Parsing fixed number failed.");
        }

      } else if (varformula != null && varformula.length() >= 2) {

        // Parameter varformula is used

        if ((varformula.charAt(0) == '/') || (varformula.charAt(0) == '*') || (varformula.charAt(0) == '+')
            || (varformula.charAt(0) == '-')) {

          operation = 10;
          operator = varformula.charAt(0);
          operandName = varformula.substring(1);

        } else if ((varformula.charAt(varformula.length() - 1) == '/')
            || (varformula.charAt(varformula.length() - 1) == '*')
            || (varformula.charAt(varformula.length() - 1) == '+')
            || (varformula.charAt(varformula.length() - 1) == '-')) {

          operation = 20;
          operator = varformula.charAt(varformula.length() - 1);
          operandName = varformula.substring(0, varformula.length() - 1);

        } else {
          throw new ConfigException("Parameter formula is mallformed: \"" + varformula + "\"");
        }

      } else {
        throw new ConfigException("Parameter formula or varformula must be defined");
      }

      if (operator!=0){
	      if (operator == '/') {
	        operation += 1;
	      } else if (operator == '*') {
	        operation += 2;
	      } else if (operator == '+') {
	        operation += 3;
	      } else if (operator == '-') {
	        operation += 4;
	      }
      }

    } catch (ConfigException ce) {
      throw ce;
    } catch (Exception e) {
      throw new ConfigException("Unexpected error during creation", e);
    }

  }

  public String getSource() throws Exception {
    return src;
  }

  public String getTarget() throws Exception {
    return tgt;
  }

  public String getName() throws Exception {
    return name;
  }
}
