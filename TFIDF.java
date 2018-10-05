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

public class TFIDF extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TFIDF.class);
  private static final String COUNT = "filecount";

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TFIDF(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Path path_ = new Path(args[0]);
    FileSystem fileSystem = FileSystem.get(getConf()); // filesystem should be configured
    ContentSummary contentSummary = fileSystem.getContentSummary(path_); // get the content summary 
    long fileCount = contentSummary.getFileCount();
    getConf().set(COUNT, ""+fileCount);

    JobControl jobControl = new JobControl("JobChain");
    Job job = Job.getInstance(getConf(), "TF"); // create a job instance and set the job name to TFIFDF
    job.setJarByClass(this.getClass());//set the jar file
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0])); // get the input path which is the argument 0
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // create a new output file based on the path given i args 1
    job.setMapperClass(Map1.class);  // set the Map class
    job.setReducerClass(Reduce1.class); //set the reducer class
    job.setOutputKeyClass(Text.class);  // set the Output Key class
    job.setOutputValueClass(DoubleWritable.class); // set the output value class
    ControlledJob controlledJob = new ControlledJob(getConf());
    controlledJob.setJob(job);
    
    jobControl.addJob(controlledJob);
    
    Job job2 = Job.getInstance(getConf(), "TFIDF");
    job2.setJarByClass(this.getClass());
    
    FileInputFormat.addInputPath(job2, new Path(args[1])); // get the input path which is the argument 0
    FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/final")); // create a new output file based on the path given i args 1
    job2.setMapperClass(Map2.class);// set the Map class
    job2.setReducerClass(Reduce2.class);//set the reducer class
    job2.setOutputKeyClass(Text.class);// set the Output Key class
    job2.setOutputValueClass(Text.class);// set the Output value class
    ControlledJob controlledJob2 = new ControlledJob(getConf());
    controlledJob2.setJob(job2);
    
    controlledJob2.addDependingJob(controlledJob);
    
    jobControl.addJob(controlledJob2);
    
    Thread jobControlThread = new Thread(jobControl);
    jobControlThread.start();
    while (!jobControl.allFinished()) {
       // System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
       // System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
       // System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
       // System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
       // System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
    try {
        Thread.sleep(5000);
        } catch (Exception e) {

        }

      } 
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	    private final static DoubleWritable one = new DoubleWritable(1);
	    private Text word = new Text();
	    private long numRecords = 0;    
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); //regex to split the line into words 

	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	      String line = lineText.toString(); // read the line 
	      Text currentWord = new Text();
	      FileSplit fileSplit = (FileSplit) context.getInputSplit();
	      String fileName = fileSplit.getPath().getName();
	      for (String word : WORD_BOUNDARY.split(line)) {
	        if (word.isEmpty()) {
	            continue;
	        }
	            currentWord = new Text(word.toLowerCase()); //if the word is present then get the lowercase of the word
		    Text fileString = new Text(currentWord+"#####"+fileName); //append #####
	            context.write(fileString, one);
	        }
	    }
	  }

	  public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    @Override
	    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
	        throws IOException, InterruptedException {
	      int sum = 0; //initialise the sum to 0
	      for (DoubleWritable count : counts) {
	        sum += count.get(); // count will be incremented for each string
	      }

	      double termFrequency = 1.00 + Math.log10(sum); //calculate the term frequency
	      context.write(word, new DoubleWritable(termFrequency)); // write the ouput to output path, add the word, delimeter and the tfidf score
	    }
	  }

  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {   
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); //regex to split the line into words 

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();

      if(line.isEmpty()) {
        return;
      }
      line = line.toLowerCase(); //if the word is present then get the lowercase of the word
      String[] splitLine = line.split("#####");
      if(splitLine.length<2) {
        return;
      }

      Text words = new Text(splitLine[0]); // get the word 
      Text wordSummary = new Text(splitLine[1]); //get the file name
      context.write(words, wordSummary); // write the output
    }
  }

  public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
      double sum = 0; //initialise the sum to 0
      double tfidf = 0; //initialise the tfidf score to 0
      long frequencyCount = context.getConfiguration().getLong(COUNT,1);
      ArrayList<Text> wordInFiles = new ArrayList<>();

      for(Text count : counts) { //run for each string in the value list
        sum++; // count will be incremented for each string
        wordInFiles.add(new Text(count.toString())); // add it in the arrayList
      }
      for (Text having : wordInFiles) {
        String[] parts = having.toString().split("\t"); // split the file name and term frequency with the delimeter tab
        double idf = Math.log10(1.0 + (frequencyCount/sum)); //calculate the Inverse Document Frequency score
        tfidf = Double.parseDouble(parts[1])*idf;  // multiply IDF and word frequency
        context.write(new Text(word+"#####"+parts[0]), new DoubleWritable(tfidf)); // write the ouput to output path, add the word, delimeter and the tfidf score
      }
    }
  }
}
