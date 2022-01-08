package fs.partitionandjoin;


public class IndexElement implements Comparable<IndexElement> {
	//private int sequence;
	private int rid;
	private int length;
	private int position;
	private int sequence;
	//the token's position in the sorted token list of each reord
	public IndexElement(int id, int len) {
		//this.sequence=seq;
		this.rid = id;
		this.length = len;
		
	}
	public IndexElement(int id, int len, int pos, int seq){
		this.rid=id;
		this.length=len;
		this.position=pos;
		this.sequence=seq;
	}
	
	public int getRID() {
		return rid;
	}

	public int getLength() {
		return length;
	}
	

	public void setRID(int id) {
		this.rid = id;
	}

	public void setLength(int len) {
		this.length = len;
	}
	public void setPosition(int pos){
		this.position=pos;
	}
	public int getPosition(){
		return this.position;
	}
	public void setSequence(int seq){
		this.sequence=seq;
	}
	public int getSequence(){
		return this.sequence;
	}

	@Override
	public int compareTo(IndexElement object) {
		// TODO Auto-generated method stub
		if (this.getLength() < object.getLength())
			return -1;
		else if (this.getLength() > object.getLength())
			return 1;
		else
			return 0;
	}

}
