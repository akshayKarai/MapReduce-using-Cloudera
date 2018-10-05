package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import org.apache.log4j.Logger;

public class Search extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Search.class);
  private static final String ARGUMENTS = "args";

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Search(), args); //Calling the run command function
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
   
    Job job = Job.getInstance(getConf(), "Search"); //create a job instance and set its name 
    job.setJarByClass(this.getClass()); //set the jar file
    
    String[] nargs = new String[args.length-2]; //read the input query string 
    for(int i=2;i<args.length;i++){
    	nargs[i-2] = args[i];
    }
    
    job.getConfiguration().setStrings(ARGUMENTS,nargs); // set the job configuration
    FileInputFormat.addInputPath(job, new Path(args[0])); // add the input file path given in args 0
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // create a new path for output, specfied in agrs2
    job.setMapperClass(Map.class);// set the mapper class

    job.setReducerClass(Reduce.class);//set the reducer class
    job.setOutputKeyClass(Text.class);// set the output key class
    job.setOutputValueClass(DoubleWritable.class);//set the output value class
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    private final static DoubleWritable one = new DoubleWritable(1);
	    private Text word = new Text(); //create a text object
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); // regex to read input file

	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	      String line = lineText.toString(); // read the line
	      Text currentWord = new Text();
	      ArrayList<Text> arrayArguments = new ArrayList<>();
	      double tfidflist = 0;
	      
	      if(line.isEmpty()){ //if line read is empty then return
	    	  return;
	      }
	      
	      line = line.toLowerCase(); // if not empty then convert it to lowercase
	      String[] splitLine = line.split("#####"); //split the line based on the refex ######
	      
	      if(splitLine.length < 2){// if the regex is between empty string then return
	    	  return;
	      }
	      
	      Text word = new Text(splitLine[0]); //else, assign the first split to word
	      Text wordDetails = new Text(splitLine[1]); // second split to WordDetails
	      
	      String[] noargs = context.getConfiguration().getStrings(ARGUMENTS);// get all the aarguments and put it into array declared global
	      boolean wordExists = false;
	      for(int i=0;i<noargs.length;i++){// loop through the word in noargs[] and den it into list array
	    	  String currentArgument = noargs[i].toLowerCase();
	    	  if(currentArgument.equalsIgnoreCase(splitLine[0])){
	    		  wordExists = true;
	    		  break;
	    	  }
	      }
	      
	      // if(!arrayArguments.contains(word)){
	    	 //  return;
	      // }

	      if(!wordExists){
	    	  return;
	      }
	      
	      String[] parts = wordDetails.toString().split("\t");
	      
	      word = new Text(parts[0]);
	      tfidflist = Double.parseDouble(parts[1]);//Text file and tf-idf value is put into intermediate file. The key will be the file and value will be tf-idf
	      context.write(word, new DoubleWritable(tfidflist)); //writing the output key-value pairs
	      
	    }
	  }

	  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    @Override
	    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
	        throws IOException, InterruptedException {
	      double sum = 0.00;
	      for (DoubleWritable count : counts) {
	        sum += count.get();//adding
	      }
	      context.write(word, new DoubleWritable(sum)); //writing the output 
	    }
	  }
}
