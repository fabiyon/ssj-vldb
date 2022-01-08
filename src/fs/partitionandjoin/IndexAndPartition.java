package fs.partitionandjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import fs.common.CommonFunctions;
import fs.common.Configure;
import fs.common.ListElement;
import fs.common.Partition;
import fs.common.SortTokens;

/**
 * Construct prefix inverted index and partition each record into multiple partitions
 * @author rct
 *
 */
public class IndexAndPartition {
	static int TotalFrequency=0;
	static HashMap<String, Integer> GlobalOrdering = new HashMap<String, Integer>();
	static Vector<SortTokens> TokenFrequency = new Vector<SortTokens>();	
	//TODO:Map
	public static class IndexAndPartitionMap extends Mapper<LongWritable,Text,CompositeKey,Text> {
		ArrayList<String> tokenlist = new ArrayList<String>();
		Vector<SortTokens> TokensVector = new Vector<SortTokens>();	
		Vector<Integer> Pivots = new Vector<Integer>();	
		
		int weight=0;
		//IntWritable newkey = new IntWritable();
		CompositeKey newkey;
		Text newvalue = new Text();			
		
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
			
			int tokenNumber = GlobalOrdering.keySet().size();
			Pivots.clear();
			if(Configure.usingTF == false)
				PivotsSelection(tokenNumber,Configure.parInitialNumber ,Pivots);
			if(Configure.usingTF == true)
				PivotsSelectionByTF(Configure.partitionNumber,Pivots);					
		}
		//TODO:Map=========================================
		public void map(LongWritable key, Text value,Context context) throws IOException,
		InterruptedException{			
			int jobID=context.getJobID().getId();
			int taskID=context.getTaskAttemptID().getTaskID().getId();
			//System.out.println(context.getJobID().getId()+" "+context.getTaskAttemptID().getId());
			String pathSuffix=String.valueOf(jobID+"_"+taskID);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			String inputline = value.toString().trim();
			// System.out.println(inputline);
			int index = inputline.indexOf(" ");
			String rid = inputline.substring(0, index);
			String content = inputline.substring(index + 1).trim();
			String transformed = CommonFunctions.TransformString2(content);
			// System.out.println(transformed);
			if (Configure.TokenAsWord && transformed.length() > 0) {
				tokenlist = CommonFunctions.SplitToTokens_Word(transformed);
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
					//TODO: Get partitions and write to HDFS				
					// initial
					int pivotsNumber=Pivots.size();
					int pivotCursor = 0;
					int tokenCursor = 0;
					// the token number in each block
					int curPar = 0, prevPar = 0, sufixPar = 0;
					StringBuffer partokbuf = new StringBuffer();
					// partition the token list into vetical slices
					//partition
					while (tokenCursor < tokensNumber && pivotCursor < pivotsNumber) {
						int tokenWeight = TokensVector.get(tokenCursor).getWeight();
						int pivotWeight = Pivots.get(pivotCursor);
						if (tokenWeight < pivotWeight) {
							partokbuf.append(tokenWeight + " ");
							curPar++;
							tokenCursor++;
							continue;
						}
						if (tokenWeight >= pivotWeight) {
							// out put the current partition
							if (curPar != 0) {
								prevPar = tokenCursor - curPar;
								sufixPar = tokensNumber - tokenCursor;								
								//out
//								String outstr=rid+" "+tokensNumber+" "+prevPar+" "+curPar+" "+sufixPar+" "+partokbuf.toString();
//								partitionBuffer[pivotCursor].append(outstr+"\n");
//								partitionBufLenth[pivotCursor]+=outstr.length();
//								if(partitionBufLenth[pivotCursor]>bufUperbound){
//									String val=partitionBuffer[pivotCursor].toString().trim();
//									newvalue.set(val);	
//									newkey.set(pivotCursor);
//									context.write(newkey, newvalue);
//									partitionBufLenth[pivotCursor]=0;
//									partitionBuffer[pivotCursor].setLength(0);
//								}
								KeyValueSet(newvalue,rid,tokensNumber,prevPar,curPar,sufixPar,partokbuf.toString());
								//newkey.set(pivotCursor);
								newkey=new CompositeKey(pivotCursor,tokensNumber,Integer.parseInt(rid));
								context.write(newkey, newvalue);								
//								System.out.println("0 pivot: "
//										+ Pivots.get(pivotCursor) + " par id: "
//										+ pivotCursor + " Partition: " + partokbuf);
								partokbuf.setLength(0);
								curPar = 0;
								prevPar = 0;
								sufixPar = 0;
							}
							pivotCursor++;
						}
					}
					if(pivotCursor < pivotsNumber && curPar!=0){
						//the last
						prevPar = tokenCursor - curPar;
						sufixPar = tokensNumber - tokenCursor;
						//out
//						String outstr=rid+" "+tokensNumber+" "+prevPar+" "+curPar+" "+sufixPar+" "+partokbuf.toString();
//						partitionBuffer[pivotCursor].append(outstr+"\n");
//						partitionBufLenth[pivotCursor]+=outstr.length();
//						if(partitionBufLenth[pivotCursor]>bufUperbound){
//							newvalue.set(partitionBuffer[pivotCursor].toString().trim());	
//							newkey.set(pivotCursor);
//							context.write(newkey, newvalue);
//							partitionBufLenth[pivotCursor]=0;
//							partitionBuffer[pivotCursor].setLength(0);
//						}
						KeyValueSet(newvalue,rid,tokensNumber,prevPar,curPar,sufixPar,partokbuf.toString());
						//newkey.set(pivotCursor);
						newkey=new CompositeKey(pivotCursor,tokensNumber,Integer.parseInt(rid));
						context.write(newkey, newvalue);
//						System.out.println("1 pivot: " + Pivots.get(pivotCursor)
//								+ " par id: " + pivotCursor + " Partition: "
//								+ partokbuf);
						partokbuf.setLength(0);
						curPar = 0;
						prevPar = 0;
						sufixPar = 0;					
					}
					if (tokenCursor < tokensNumber) {
						// the left part
						while (tokenCursor < tokensNumber) {
							int tokenWeight = TokensVector.get(tokenCursor)
									.getWeight();
							partokbuf.append(tokenWeight + " ");
							tokenCursor++;
							curPar++;
						}
						prevPar = tokenCursor - curPar;
						sufixPar = tokensNumber - tokenCursor;
						//out
//						String outstr=rid+" "+tokensNumber+" "+prevPar+" "+curPar+" "+sufixPar+" "+partokbuf.toString();
//						partitionBuffer[pivotCursor].append(outstr+"\n");						
//						partitionBufLenth[pivotCursor]+=outstr.length();
//						if(partitionBufLenth[pivotCursor]>bufUperbound){
//							newvalue.set(partitionBuffer[pivotCursor].toString().trim());		
//							newkey.set(pivotCursor);
//							context.write(newkey, newvalue);
//							partitionBufLenth[pivotCursor]=0;
//							partitionBuffer[pivotCursor].setLength(0);
//						}
						KeyValueSet(newvalue,rid,tokensNumber,prevPar,curPar,sufixPar,partokbuf.toString());
						//newkey.set(pivotCursor);
						newkey=new CompositeKey(pivotCursor,tokensNumber,Integer.parseInt(rid));
						context.write(newkey, newvalue);
//						System.out.println("2 pivot: "  
//								+ "> pivot; par id: " + pivotCursor + " Partition: "
//								+ partokbuf);
						partokbuf.setLength(0);
						curPar = 0;
						prevPar = 0;
						sufixPar = 0;
					}				
				}
				TokensVector.clear();				
			}	
		}
		//TODO:cleanup=================================
		public void cleanup(Context context) throws IOException,InterruptedException{
			GlobalOrdering.clear();	
			TokenFrequency.clear();			
			Pivots.clear();			
			System.out.println("Clean Up Complete");
	}
}
	//TODO:Reduce-----------------------------------------------
	public static class IndexAndPartitionReduce extends Reducer<CompositeKey,Text,RidLenPairKey,IntWritable>{
		InvertedLists prefixIndex =new InvertedLists();
		ArrayList<Partition> partitionsGroup = new ArrayList<Partition>();
		Vector<ListElement> toSort = new Vector<ListElement>();
		Iterator<ListElement> prefixIter=null;
		HashMap<Integer,Integer> Sequence2Rid=new HashMap<Integer,Integer>();
		int sequence=0;
                double similarityThreshold;
		protected  void setup(Context context)  throws IOException,InterruptedException{	
			Configuration conf = context.getConfiguration();
			if (Configure.RunOnCluster == true) {			
					TotalFrequency = (int) CommonFunctions.LoadToAllNodesReduce(conf, toSort, Sequence2Rid);
				} else {// run on single node
					TotalFrequency = (int) CommonFunctions.LoadToOneNodeReduce(conf,toSort, Sequence2Rid);
				}
			prefixIter=toSort.iterator();
			System.out.println("entry of List: "+TotalFrequency);		
                        similarityThreshold = conf.getDouble("similarityThreshold", 0);
		}
		//TODO:reduce======================================
		public void reduce(CompositeKey key,Iterable<Text> values, Context context) throws IOException,InterruptedException{
			System.out.println("partition id: "+key.getID());
			System.out.println("start join");
			Iterator<Text> iter=values.iterator();
			//reset
			prefixIter=toSort.iterator();
			while(iter.hasNext()){
			//System.out.println("Test: "+inputLine);
			Partition par=new Partition(iter.next().toString());
			//System.out.println("rid: "+par.getRid());
			JoinProcess(par,sequence,context);
			//keep in memory
			partitionsGroup.add(par);			
			Sequence2Rid.put(sequence, par.getRid());
			sequence++;				
			}
			//rest
			partitionsGroup.clear();
			Sequence2Rid.clear();
			prefixIndex.clear();
			sequence=0;
			System.out.println("end join");
		}
		public void JoinProcess(Partition par,int sequenceX, Context context) throws IOException, InterruptedException{
			// get the partition information				
			int ridX = par.getRid();				
			//System.out.println("TO RIDX:"+ridX+" sequenceY: "+sequenceX);				
			int lengthX = par.getLength();
			//System.out.println("sequenceX��"+sequenceX+" ridX: "+ridX+" lengthX: "+lengthX);
			ListElement prefixInfo=null;
			while(prefixIter.hasNext()){
				prefixInfo=prefixIter.next();
				//int tmp=prefixInfo.getRID();
				//System.out.println(ridX+":"+sequenceX+":"+tmp);
				if(prefixInfo.getRID()==ridX)
					break;
			}			
			//System.out.println("equal:"+ridX+":"+prefixInfo.getRID());
                        if (prefixInfo == null) {
                          System.out.println("Couldn't find necessary prefix for rid " + ridX);
                          return;
                        }
			int [] prefixTokens=prefixInfo.getPrefix();	
			int probeIndex=prefixTokens.length;
			int indexLength=CommonFunctions.getIndexPrefixLength(lengthX, similarityThreshold);	
			int lengthLowerBound=(int) (lengthX * similarityThreshold);
			//int probeIndex=getProbeIndex(lengthX);
			//System.out.println(lengthX+" :probe Index:"+probeIndex+" index : "+prefixTokens.size());
			
			//System.out.println(ridX+" "+lengthX);
			final HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>();
					
			for (int posX = 0; posX < probeIndex; posX++) {
				// get list for token i
				//System.out.println("prefix:"+prefixTokens[i]);
				Vector<IndexElement> list = prefixIndex.getList(prefixTokens[posX]);					
				if (list != null) {
					// get start position for each loop
					//System.out.println("list size: "+list.size());
					int start = prefixIndex.getStartPosition(prefixTokens[posX]);
					int shift = 0;
					//System.out.println("start: " + start);
					// start from the given position to the exact position
					Iterator<IndexElement> iter = list.listIterator(start);
					while (iter.hasNext()) {
						IndexElement elem = iter.next();
						if (elem.getLength() < lengthLowerBound) {
							//iter.next();
							shift = shift + 1;
						} else {
							if (shift == 0){
								break;
								}
							else {
								start = start + shift;
								prefixIndex.setStartPosition(prefixTokens[posX], start);
								//System.out.println("new start: " + start);
								break;
							}
						}
					}// end while move to the exact position
					// get the candidates, then to count the co-occurence
					//reset to start position
					iter = list.listIterator(start);
					while (iter.hasNext()) {
						IndexElement elem = iter.next();
						int ridY = elem.getRID();
						int posY=elem.getPosition();
						int lengthY=elem.getLength();
						int sequenceY=elem.getSequence();
						
						Integer count = counts.get(sequenceY);
	                    if (count == null) {
	                        count = 0;
	                    }
	                    if (count != -1) {
	                        count++;
	                        // position filter
	                        if (!CommonFunctions.passPositionFilter(count,
	                        		posX, lengthX, posY,
	                        		lengthY, similarityThreshold)) {
	                            count = -1;
	                        }
	                        // suffix filter  block
	                        if (count != -1) {
	                        	int comm=CommonFunctions.CommonUpperBound(par,partitionsGroup.get(sequenceY));
	                        	if(comm<lengthLowerBound)
	                        		count = -1;
	                        }
	                        counts.put(sequenceY, count);
	                    }
					}// end get candidates
				}// end if list!=null
				//TODO:add the prefix tokens of the current partition into index
				//if(posX<indexLength)
				//	prefixIndex.add2List(prefixTokens[posX], ridX, lengthX, posX, sequenceX);
			}// end for i count	
			for(int p=0;p<indexLength;p++){
				prefixIndex.add2List(prefixTokens[p], ridX, lengthX, p, sequenceX);
			}	
			// TODO: out put the result pairs,implement the codes
			for(Map.Entry<Integer, Integer> cand : counts.entrySet()){
				int count=cand.getValue();
				if(count<0)
					continue;
				int sequenceY=cand.getKey();
				Partition parY=partitionsGroup.get(sequenceY);
				int ridY=parY.getRid();
				int lengthY=parY.getLength();
				int common=CommonFunctions.getCommonTokensNum(par.getTokens(),parY.getTokens()); 
				if(common<=0)
					continue;
				if(ridX<= ridY){
					//String []infoY=can.gettoken().split(",");						
					RidLenPairKey newkey=new RidLenPairKey(ridX,lengthX,ridY,lengthY);
					IntWritable newvalue=new IntWritable();						
					newvalue.set(common);
					context.write(newkey, newvalue);
				}
				else{
					RidLenPairKey newkey=new RidLenPairKey(ridY,lengthY,ridX,lengthX);
					IntWritable newvalue=new IntWritable();
					newvalue.set(common); // das ist der Overlap	
					context.write(newkey, newvalue);
				}					
					//System.out.println("key: "+key+" "+newkey.toString()+" "+newvalue.toString());
			}					
	}
		//TODO:cleanup======================================
		public void cleanup(Context context){
			System.out.println("partition group size: "+partitionsGroup.size());
			partitionsGroup.clear();
			prefixIndex.clear();
			Sequence2Rid.clear();
			sequence=0;
			toSort.clear();
		}
	}
	
        public static class IndexAndPartitionStatisticsReduce extends Reducer<CompositeKey, Text, CompositeKey, Text> {
          Text outVal = new Text();
          public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            Iterator<Text> iter = values.iterator();
            int count = 0;
            while(iter.hasNext()) {
              Text currentElt = iter.next();
              if (currentElt.toString().contains(" ")) { // das ist dann noch ein Original-Record aus dem Mapper
                count++; 
              } else {
                count += Integer.parseInt(currentElt.toString());
              }
            }
            outVal.set(count + "");
            context.write(key, outVal);
          }
	}
        
	/**
	 * select pivots using global ordering, the math formulation
	 * @param tokenNumber
	 * @param Pivots store the selected pivots
	 */
	public static void PivotsSelection(int tokenNumber, int firstParNumber, Vector<Integer> Pivots) {
		int dishu=3;
		//the first part
		int p1 =(int) (tokenNumber*0.95);
		//Pivots.add(p1);
		int pivot=p1;
		int count=1;
		int temp=0;
		System.out.println("pivot0: "+pivot);
		while(temp<tokenNumber){
			Pivots.add(pivot);		
			count=count+1;
			//pivot=(int) (tokenNumber*(0.95+0.005*count));			
			//pivot=(int)(tokenNumber*(1-(Math.pow(dishu, count)/divide)));
			pivot=(int) (pivot+(tokenNumber-p1)/(dishu*count));
			//Pivots.add(pivot);			
			System.out.println("pivot: "+pivot);
			temp=pivot;
		}
		Collections.sort(Pivots);
		//Configure.PivotsNumber=count;
		System.out.println("Pivots Number is: " + Pivots.size());
		System.out.println("Pivots are: " + Pivots.toString());
	}
	/**
	 * Partition by the even token frequency
	 * @param PartitionNumber
	 * @param Pivots
	 */
	public static void PivotsSelectionByTF(int PartitionNumber, Vector<Integer> Pivots){
		int totalFQ=0;
		int evenFQ=0;		
		evenFQ=(int) Math.ceil((double)TotalFrequency/(double)PartitionNumber);
		for(int i=0;i<TokenFrequency.size();i++){
			int fq=TokenFrequency.get(i).getWeight();
			totalFQ+=fq;
			if(totalFQ>=evenFQ){
				String key=TokenFrequency.get(i).gettoken();
				int pivot=GlobalOrdering.get(key);
				Pivots.add(pivot);
				totalFQ=0;
			}
		}
		Collections.sort(Pivots);
		System.out.println("Pivots Number is: " + Pivots.size());
		System.out.println("Pivots are: " + Pivots.toString());
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
	
	
	/**
	 * Set newkey and newvalue for out put
	 * @param newkey
	 * @param newvalue
	 * @param pivotCursor
	 * @param rid
	 * @param tokensNumber
	 * @param prevPar
	 * @param curPar
	 * @param sufixPar
	 * @param partition
	 * @param prefixTokens
	 */
	public static void KeyValueSet(Text newvalue,String rid, int tokensNumber,int prevPar,int curPar,int sufixPar,String partition){
		//newkey.set(pivotCursor);
		//newkey=new CompositeKey(pivotCursor,tokensNumber);
		newvalue.set(rid + " " + tokensNumber + " " + prevPar + " "
				+ curPar + " " + sufixPar + " "
				+ partition);
//		newvalue.set(rid + " " + tokensNumber + " " + prevPar + " "
//				+ curPar + " " + sufixPar + " "
//				+ partition + prefixTokens);
	}
		
}
