package fs.generateprefixfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import fs.common.CommonFunctions;
import fs.common.Configure;
import fs.common.SortTokens;

public class GeneratePrefixFile {
		
	public static class GeneratePrefixFileMap extends Mapper<LongWritable,Text,prefixKey,Text>{
		public int TotalFrequency=0;
		public HashMap<String, Integer> GlobalOrdering = new HashMap<String, Integer>();
		public Vector<SortTokens> TokenFrequency = new Vector<SortTokens>();
		Vector<SortTokens> TokensVector = new Vector<SortTokens>();	
		ArrayList<String> tokenlist = new ArrayList<String>();
		Text newvalue=new Text();
                double similarityThreshold;
		//TODO:Setup===================================
		public void setup(Context context) throws IOException,
		InterruptedException{
			//load dictionary
			GlobalOrdering.clear();
			TokenFrequency.clear();
			
			Configuration conf = context.getConfiguration();
			if (Configure.RunOnCluster == true) {
				TotalFrequency=(int) CommonFunctions.LoadToAllNodesMap(conf, GlobalOrdering,TokenFrequency);
			} else {// run on single node
				TotalFrequency=(int) CommonFunctions.LoadToOneNodeMap(conf, GlobalOrdering,TokenFrequency);
			}
                        similarityThreshold = conf.getDouble("similarityThreshold", 0);
		}
		public void cleanup(Context context){
			GlobalOrdering.clear();
			TokenFrequency.clear();
		}
		//TODO: map
		public void map(LongWritable Key, Text Input,Context context) throws IOException,InterruptedException{
		
			String inputline = Input.toString().trim();
			int index = inputline.indexOf(" ");
			String rid = inputline.substring(0, index);
			String content = inputline.substring(index + 1).trim();
			String transformed = CommonFunctions.TransformString2(content);
			tokenlist = CommonFunctions.SplitToTokens_Word(transformed);
			int weight=0;
			for (String token : tokenlist) {
				weight = GlobalOrdering.get(token);
				SortTokens sorttoken = new SortTokens(token, weight);
				TokensVector.add(sorttoken);					
			}
			
			int tokensNumber = TokensVector.size();			
			
			if (tokensNumber > 0) {
				//TODO: prefix index selectio and write out
				// sort tokens
				Collections.sort(TokensVector);
				// get prefix tokens
				String prefixTokens = getIndexPrefixTokens(TokensVector, similarityThreshold);
				newvalue.set(prefixTokens);
				prefixKey newKey= new prefixKey(tokensNumber,Integer.parseInt(rid));
				context.write(newKey, newvalue);				
			}
			TokensVector.clear();
		}			
	}
	public static class GeneratePrefixFileReduce extends Reducer<prefixKey,Text,Text,Text>{
		Text newkey=new Text();
		public void reduce(prefixKey Key,Iterable<Text> Values, Context context) throws IOException,InterruptedException{
			Iterator<Text> iter=Values.iterator();
			
			while(iter.hasNext()){
				Text newvalue=iter.next();
				newkey.set(Key.toString());				
				context.write(newkey,newvalue );
			}
		}
	}
	/**
	 * Get Prefix Tokens
	 * @param tokensWeight
	 * @return
	 */
	public static String getIndexPrefixTokens(Vector<SortTokens> tokensWeight, double similarityThreshold){
		StringBuffer strBuf=new StringBuffer();
		int count=tokensWeight.size();
		int prefixLength=count-(int)Math.ceil((double)count * similarityThreshold)+1;
		//int prefixLength=getIndexPrefixLength(count);
		for(int i=0;i<prefixLength;i++){
			strBuf.append(tokensWeight.get(i).getWeight()+" ");
		}
		return strBuf.toString();
	}
}
