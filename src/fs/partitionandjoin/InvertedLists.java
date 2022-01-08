package fs.partitionandjoin;


import java.util.HashMap;
import java.util.Vector;


public class InvertedLists {
	private HashMap<Integer, Vector<IndexElement>> lists;
	private HashMap<Integer, Integer> startPosition;

	public InvertedLists() {
		lists = new HashMap<Integer, Vector<IndexElement>>();
		startPosition = new HashMap<Integer, Integer>();
	}
	public int listEntryNum(){
		return lists.size();
	}
	public void resetStartPosition(){
		//int num=startPosition.size();
		for(Integer token:startPosition.keySet()){
			startPosition.put(token, 0);
		}
	}

	public Vector<IndexElement> getList(int token) {
		return lists.get(token);
	}

	public int getStartPosition(int token) {
		return startPosition.get(token);
	}
	public void setStartPosition(int token,int newStart){
		startPosition.put(token, newStart);
	}
	public void add2List(int token,int rid,int len,int pos,int seq){
		IndexElement elem=new IndexElement(rid,len,pos,seq);
		Vector<IndexElement> vec=lists.get(token);
		if(vec!=null){
			//insert to the existing list
			vec.add(elem);
		}
		else{
			//the first element in the list, creat and insert
			Vector<IndexElement> vecTmp= new Vector<IndexElement>(); 
			vecTmp.add(elem);
			lists.put(token, vecTmp);
			startPosition.put(token, 0);
		}
	}	
	public void clear(){
		for(Integer key:lists.keySet()){
			Vector<IndexElement> list=lists.get(key);
			list.clear();
		}
		lists.clear();
		startPosition.clear();
	}
}
