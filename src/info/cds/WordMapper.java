package info.cds;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	Text reusableText=new Text();
	IntWritable one=new IntWritable(1);
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		String s=value.toString();
		for(String word:s.split(" ")){
		
			if(word.length()>0){
			     reusableText.set(word);
				context.write(reusableText,one);
				
			}
		}
	}

}

