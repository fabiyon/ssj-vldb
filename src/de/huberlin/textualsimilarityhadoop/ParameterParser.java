/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.textualsimilarityhadoop;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author fabi
 */
public class ParameterParser {
  private String theta;
  private String input; // R (self join, RxS join)
  private String inputS; // S (RxS join)
  private String output;
  private int numPartitions;
  private int numReducers;
  private int memory;
  private int numberOfPivots;
  private int numberOfSamples;
  private int modulo;
  private String dataType;
  
  public ParameterParser(String[] args) throws Exception {
    // create Options object
    Options options = new Options();
    options.addOption(OptionBuilder
                		.withLongOpt("input")
                		.withDescription("input path R")
                		.isRequired()
                		.withValueSeparator('=')
                		.hasArg()
                		.create("i"));
    options.addOption(OptionBuilder
                		.withLongOpt("inputS")
                		.withDescription("input path S")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("b"));
    options.addOption(OptionBuilder
                		.withLongOpt("output")
                		.withDescription("output path")
                		.isRequired()
                		.withValueSeparator('=')
                		.hasArg()
                		.create("o"));
    options.addOption(OptionBuilder
                		.withLongOpt("theta")
                		.withDescription("similarity threshold")
                		.isRequired()
                		.withValueSeparator('=')
                		.hasArg()
                		.create("t"));
    options.addOption(OptionBuilder
                		.withLongOpt("memory")
                		.withDescription("MRSimJoin: maximum memory before split in MB; V-SMART: max number of records in one chunk")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("m"));
    options.addOption(OptionBuilder
                		.withLongOpt("numPartitions")
                		.withDescription("MRSimJoin & ClusterJoin: number of partitions in each iteration/ partition size")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("p"));
    options.addOption(OptionBuilder
                		.withLongOpt("numReducers")
                		.withDescription("MRSimJoin: number of reducers")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("r"));
    options.addOption(OptionBuilder
                		.withLongOpt("numberOfPivots")
                		.withDescription("ClusterJoin: number of pivots")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("n"));
    options.addOption(OptionBuilder
                		.withLongOpt("numberOfSamples")
                		.withDescription("ClusterJoin: number of samples")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("s"));
    
    options.addOption(OptionBuilder
                		.withLongOpt("modulo")
                		.withDescription("ClusterJoin: modulo")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("d"));
    
    options.addOption(OptionBuilder
                		.withLongOpt("dataType")
                		.withDescription("VernicaJoin: End-To-End version needs additional information")
                		.withValueSeparator('=')
                		.hasArg()
                		.create("e"));
    
    CommandLineParser parser = new BasicParser(); //DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException pvException) {
      System.out.println(pvException.getMessage());
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "FullFilteringNaiveJob", options );
      throw new Exception("missing parameters");
    }

    theta = cmd.getOptionValue("theta");
    input = cmd.getOptionValue("input");
    if (cmd.hasOption("inputS")) {
      inputS = cmd.getOptionValue("inputS");
    }
    output = cmd.getOptionValue("output");
    if (cmd.hasOption("memory")) {
      memory = Integer.parseInt(cmd.getOptionValue("memory"));
    }
    if (cmd.hasOption("numPartitions")) {
      numPartitions = Integer.parseInt(cmd.getOptionValue("numPartitions"));
    }
    if (cmd.hasOption("numReducers")) {
      numReducers = Integer.parseInt(cmd.getOptionValue("numReducers"));
    }
    if (cmd.hasOption("numberOfPivots")) {
      numberOfPivots = Integer.parseInt(cmd.getOptionValue("numberOfPivots"));
    }
    if (cmd.hasOption("numberOfSamples")) {
      numberOfSamples = Integer.parseInt(cmd.getOptionValue("numberOfSamples"));
    }
    if (cmd.hasOption("modulo")) {
      modulo = Integer.parseInt(cmd.getOptionValue("modulo"));
    }
    dataType = cmd.getOptionValue("dataType");
  }
  
  public String getTheta() {
    return theta;
  }
  public String getInput() {
    return this.input;
  }
  public String getInputS() {
    return this.inputS;
  }
  public String getOutput() {
    return this.output;
  }
  public int getMemory() {
    return this.memory;
  }
  public int getNumReducers() {
    return this.numReducers;
  }
  public int getNumPartitions() {
    return this.numPartitions;
  }
  public int getNumberOfPivots() {
    return this.numberOfPivots;
  }
  public int getNumberOfSamples() {
    return this.numberOfSamples;
  }
  public int getModulo() {
    return this.modulo;
  }
  public String getDataType() {
    return dataType;
  }
}
