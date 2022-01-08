package fs.getridpairs;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import fs.common.Configure;
import org.apache.hadoop.conf.Configuration;

public class getRIDPairs {
	public static class RIDPairsMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		Text newkey=new Text();
		IntWritable newvalue=new IntWritable();
		int count=0;
		public void map(LongWritable Key, Text Value, Context context)throws IOException,InterruptedException{
			String input=Value.toString();
			String []splits=input.split("\t");
			newkey.set(splits[0]); // Das Format müsste sein: RID1, Length1, RID2, Length2
			count=Integer.parseInt(splits[1]); // der Overlap (pro Fragment)			
			newvalue.set(count);
			context.write(newkey, newvalue);
		}		
	}
	public static class RIDPairsCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
		IntWritable newvalue=new IntWritable();
		public void reduce(Text Key, Iterable<IntWritable> Values, Context context) throws IOException,InterruptedException{
			Iterator<IntWritable> iter=Values.iterator();
			int count=0;
			while(iter.hasNext()){
				count+=iter.next().get();				
			}
			newvalue.set(count);
			context.write(Key, newvalue);
		}
		
	}
	public static class RIDPairsReduce extends Reducer<Text,IntWritable,Text,Text>{
		Text newkey=new Text();
		Text newvalue=new Text();
		DecimalFormat df = new DecimalFormat("#.00");
                double similarityThreshold;
                protected  void setup(Context context)  throws IOException,InterruptedException{	
			Configuration conf = context.getConfiguration();	
                        similarityThreshold = conf.getDouble("similarityThreshold", 0);
		}
		public void reduce(Text Key, Iterable<IntWritable> Values, Context context)throws IOException,InterruptedException{
			int count=0;
			String []splits=Key.toString().split(" ");
			String []lengths=splits[1].split(",");
			int len1=Integer.parseInt(lengths[0]);
			int len2=Integer.parseInt(lengths[1]);			
			Iterator<IntWritable> iter=Values.iterator();
			while(iter.hasNext()){
				count+=iter.next().get();				
			}
			double sim=(double)count/(double)(len1+len2-count);
			if(sim>similarityThreshold){ // Our logic looks for strictly > (before it was >=)
				newkey.set(splits[0].replace(",", " "));
//				newvalue.set(String.valueOf(df.format(sim)));
				context.write(newkey, newvalue);
			}				
		}
	}
}
