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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.log4j.Logger;

public class DocWordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(DocWordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new DocWordCount(), args);//Toolrunner used to run classes. We call WordCount function to run driver class
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "docwordcount");// create a new job to run the program and set the job name
    job.setJarByClass(this.getClass());//set the jar class
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0])); //args 1 will be the input file 
    FileOutputFormat.setOutputPath(job, new Path(args[1])); //new file has to be created at path specified in args2
    job.setMapperClass(Map.class); //set map class
    job.setReducerClass(Reduce.class); //set reducer class
    job.setOutputKeyClass(Text.class);//set the type of output key
    job.setOutputValueClass(IntWritable.class);//set the type of output value
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();// read the text line
      Text currentWord = new Text();// create a new Text object to hold the word
      FileSplit fileSplit = (FileSplit) context.getInputSplit();// get the file split 
      String fileName = fileSplit.getPath().getName(); // get the filaname
      for (String word : WORD_BOUNDARY.split(line)) {// read word by word split by regex given above
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word.toLowerCase());// if you find a word, make it lowercase
	    Text fileString = new Text(currentWord+"#####"+fileName);// append the respective file name of the word and den keep it in new Text object
            context.write(fileString, one);// write the output of map
        }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) { // iterate over each value in the value list of a particular key
        sum += count.get(); // sum the no. of occurances of the values
      }
      context.write(word, new IntWritable(sum)); //writes the output
    }
  }
}
