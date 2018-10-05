package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.log4j.Logger;

public class TermFrequency extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TermFrequency.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TermFrequency(), args);// Run with toolrunner 
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "termfrequency");//create a job instance to run d job
    job.setJarByClass(this.getClass()); // set the jar class
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0])); // get args[0] where the input file is in argument 1
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // create a new path for output file given by argument2
    job.setMapperClass(Map.class);//set map class
    job.setReducerClass(Reduce.class);// set reducer class
    job.setOutputKeyClass(Text.class);// set the type of keyclass
    job.setOutputValueClass(DoubleWritable.class);// set the type of value class
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final static DoubleWritable one = new DoubleWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); //regex to read the word  

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString(); // get a  textline 
      Text currentWord = new Text();// create a Text object to hold the textline word later
      FileSplit fileSplit = (FileSplit) context.getInputSplit();// get the file split object
      String fileName = fileSplit.getPath().getName(); //get the file name
      for (String word : WORD_BOUNDARY.split(line)) { // split word based on the given regex
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word.toLowerCase()); //if you find the word then convert it to lowercase
	    Text fileString = new Text(currentWord+"#####"+fileName); // append the respective file name where the word is present
           // context.write(currentWord,one);
            context.write(fileString, one);
        }
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;// initialise the sum to 0 
      for (DoubleWritable count : counts) {
        sum += count.get();// get the count of the number of values of a particular word_filename key
      }

      double termFrequency = 1.00 + Math.log10(sum);//get the log value which is required to calcualte the term frequency
      context.write(word, new DoubleWritable(termFrequency));// calculate the term frequency
    }
  }
}
