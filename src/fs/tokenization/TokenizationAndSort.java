package fs.tokenization;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import fs.common.CommonFunctions;
import fs.common.Configure;
import fs.common.SortTokens;


/**
 * tokenization each input line into words, then get the term frequency and sort
 * them Input: file from HDFS Output: sorted tokens according to ascending
 * ordering of TF
 * 
 * @author rct
 * 
 */
public class TokenizationAndSort {
	public static class TokenizationAndSortMap extends
			Mapper<LongWritable, Text, Text, Text> {
		Text newkey = new Text();
		Text newvalue = new Text();
		ArrayList<String> tokenlist = new ArrayList<String>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			newvalue.set("1");
			String inputline = value.toString().trim();
			// System.out.println(inputline);
			int index = inputline.indexOf(" ");
			String content = inputline.substring(index + 1).trim();
			String transformed = CommonFunctions.TransformString2(content);
			// System.out.println(transformed);
			if (Configure.TokenAsWord && transformed.length() > 0) {
				tokenlist = CommonFunctions.SplitToTokens_Word(transformed);
				for (String token : tokenlist) {
					newkey.set(token);
					context.write(newkey, newvalue);
				}
			}			
		}
	}
	public static class TokenizationAndSortCombiner extends Reducer<Text,Text,Text,Text>{
		Text newvalue=new Text();		 
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int count=0;
			Iterator<Text> iter=values.iterator();
			while(iter.hasNext()){
				int tmp=Integer.parseInt(iter.next().toString());
				count+=tmp;
			}
			newvalue.set(String.valueOf(count));
			context.write(key, newvalue);
		}		
	}
	public static class TokenizationAndSortReduce extends Reducer<Text,Text,Text,IntWritable>{
		IntWritable newvalue = new IntWritable();
		Text newkey=new Text();
		int gramtimes = 0;
		String value_str = null;		
		Vector<SortTokens> Dictionary=new Vector<SortTokens>();
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			gramtimes = 0;
			while (values.iterator().hasNext()) {
				value_str = values.iterator().next().toString();
				gramtimes += Integer.parseInt(value_str);
			}
			SortTokens st=new SortTokens(key.toString(),gramtimes);
			Dictionary.add(st);		
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			Collections.sort(Dictionary);
			int count=Dictionary.size();
			for(int i=0;i<count;i++){
				SortTokens st=Dictionary.get(i);
				newkey.set(st.gettoken());
				newvalue.set(st.getWeight());
				context.write(newkey, newvalue);
			}
		}
	}
}
