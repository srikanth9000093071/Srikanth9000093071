package info.cds;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.mapped.Configuration;


public class StopWordsDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new StopWordsDriver(), args);
		System.exit(exitCode);
	}


	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Plz provide proper input and out....");
			return -1;
		}
		org.apache.hadoop.conf.Configuration conf= getConf();
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = Job.getInstance(conf);
		job.setJarByClass(StopWordsDriver.class);
		job.setJobName("Word Counter With Stop Words Removal");
		
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		Path path1=new Path(args[1]);
		FileSystem.get(conf).delete(path1,true);
		FileOutputFormat.setOutputPath(job, path1);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set the MapClass and ReduceClass in the job
		job.setMapperClass(StopWordsMapper.class);
		job.setReducerClass(StopWordsReducer.class);
		
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
	   //job.addCacheFile(new URI("/user/kiran/stopwords.txt"));
		job.addCacheFile(new Path(args[2]).toUri());
		//Wait for the job to complete and print if the job was successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		
		return returnValue;
	}
}
