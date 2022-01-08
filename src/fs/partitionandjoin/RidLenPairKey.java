package fs.partitionandjoin;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class RidLenPairKey implements  WritableComparable<RidLenPairKey> {
//	private int ridX;
//	private int lenX;
//	private int ridY;	
//	private int lenY;
	private String keys;	
	public RidLenPairKey(){
		
	}
	public RidLenPairKey(String Ks){
		this.keys=Ks;
	}
	public RidLenPairKey(int X,int lenX,int Y,int lenY){
		int rid1=X;
		int rid2=Y;
		//int l1=Integer.parseInt(lenX);
		//int l2=Integer.parseInt(lenY);
		if(rid1<rid2){			
			this.keys=X+","+Y+" "+lenX+","+lenY;
		}
		else{
			this.keys=Y+","+X+" "+lenY+","+lenX;
		}
	}
	public String getRids(){
		String [] tokens=keys.split(" ");
		return tokens[0];
	}
	public String getLens(){
		String [] tokens=keys.split(" ");
		return tokens[1];
	}

	public int compareTo(RidLenPairKey object) {
		// TODO Auto-generated method stub		
		//System.out.println(rids+" "+object.rids+" "+rids.compareTo(object.rids));
		int res=keys.compareTo(object.keys);
		return res;
//		if(res>0)
//			return 1;
//		else if(res==0)
//			return 0;
//		else return -1;
	}
	
	public void readFields(DataInput In) throws IOException {
		// TODO Auto-generated method stub		
		keys=In.readUTF();
	}
	
	public void write(DataOutput Out) throws IOException {
		// TODO Auto-generated method stub		
		Out.writeUTF(keys);
	}
	@Override
	public String toString() {
		return keys;
	}
}
