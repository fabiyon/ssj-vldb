package fs.common;

public class ListElement implements Comparable<ListElement> {
	private int rid;
	private int length;
	private int []prefix;
	public ListElement(int id,int len,int [] tokens){
		this.rid=id;
		this.length=len;
		int size=tokens.length;
		//this.prefix=tokens;
		this.prefix=new int[size];		
		for(int i=0;i<size;i++)
			this.prefix[i]=tokens[i];
	}
	
	public int getRID(){
		return rid;
	}
	public int [] getPrefix(){
		return prefix;
	}
	public int getLength(){
		return length;
	}

	@Override
	public int compareTo(ListElement elem) {
		// TODO Auto-generated method stub
		if(this.length!=elem.length)
			return this.length-elem.length;		
		if(this.length==elem.length){			
			return this.rid-elem.rid;
			}
		return 0;
	}

}
