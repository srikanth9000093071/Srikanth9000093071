package info.cds;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		int count=0;
		for(IntWritable values:value){
			int i=values.get();
			count=count+i;
		}
		context.write(new IntWritable(count),key);
	}

}
