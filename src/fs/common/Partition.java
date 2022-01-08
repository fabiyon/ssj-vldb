package fs.common;

import java.util.Arrays;
import java.util.StringTokenizer;

public class Partition implements Comparable<Partition> {
	private int rid;
	private int length;
	private int prevBlk;
	private int currBlk;
	private int sufixBlk;
	private int[] tokensWeight;
	//private int [] prefixTokens;

	public Partition() {
		this.rid=-1;
		this.length=-1;
		this.prevBlk=-1;
		this.currBlk=-1;
		this.sufixBlk=-1;
		this.tokensWeight=null;
		//this.prefixTokens=null;
	}
	
	public Partition(String tokens){
		//System.out.println(tokens);
		StringTokenizer tokenArray = new StringTokenizer(tokens);
		rid=Integer.parseInt(tokenArray.nextToken());
		length=Integer.parseInt(tokenArray.nextToken());
		prevBlk=Integer.parseInt(tokenArray.nextToken());
		currBlk=Integer.parseInt(tokenArray.nextToken());
		sufixBlk=Integer.parseInt(tokenArray.nextToken());
		
		this.tokensWeight = new int[currBlk];
		for(int i=0;i<currBlk;i++){
			int weight = Integer.parseInt(tokenArray.nextToken());
			this.tokensWeight[i] = weight;
		}
	}

	public Partition(int rid, int len, int prev,int curr,int sufix, int[] tokensWeight,int[] prefixTokens) {
		this.rid = rid;
		this.length = len;
		this.prevBlk = prev;
		this.currBlk = curr;
		this.sufixBlk = sufix;
		this.tokensWeight = tokensWeight;		
	}

	public Partition(String rid, String len, String prev, String curr, String sufix, String tokensWeight, String prefixTokens) {
		this.rid = Integer.parseInt(rid);
		this.length=Integer.parseInt(len);
		this.prevBlk=Integer.parseInt(prev);
		this.currBlk = Integer.parseInt(curr);
		this.sufixBlk = Integer.parseInt(sufix);
		
		StringTokenizer tokens = new StringTokenizer(tokensWeight);
		this.tokensWeight = new int[currBlk];
		int i = 0;
		while (tokens.hasMoreTokens()) {
			int weight = Integer.parseInt(tokens.nextToken());
			this.tokensWeight[i] = weight;
			i = i + 1;
		}
	}

	public int getRid() {
		return rid;
	}

	public void setRid(int id) {
		this.rid = id;
	}
	public int getLength(){
		return length;
	}
	public void setLength(int len){
		this.length=len;
	}
	
	public int getPrev(){
		return prevBlk;
	}
	
	public void setPrev(int prev){
		this.prevBlk=prev;
	}

	public int getCurr() {
		return currBlk;
	}

	public void setCurr(int curr) {
		this.currBlk = curr;
	}
	
	public int getSufix(){
		return sufixBlk;
	}
	public void setSufix(int sufix){
		this.sufixBlk = sufix;
	}
	public int[] getTokens(){
		return tokensWeight;
	}
	public void setTokens(int[] tokens){
		int count=tokens.length;
		tokensWeight=new int[count];
		for(int i=0;i<count;i++){
			tokensWeight[i]=tokens[i];
		}
	}
	
	@Override
	public String toString() {
		return rid + " " +length+" "+prevBlk+" "+currBlk + " " +sufixBlk+" "+Arrays.toString(tokensWeight);//+" "+Arrays.toString(prefixTokens);
	} 
	
	@Override
	public int compareTo(Partition object) {
		// TODO Auto-generated method stub
		int res=this.getLength()-object.getLength();
		if(res!=0)
			return res;
		if (res==0) {
			return this.getRid()-object.getRid();
		}		
		return 0;
	}
}
