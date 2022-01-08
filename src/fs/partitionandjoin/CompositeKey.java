package fs.partitionandjoin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKey implements  WritableComparable<CompositeKey> {
	private int parID;
	private int length;
	private int RID;
	
	public static class Comparator extends WritableComparator{
     protected Comparator() {
			super(CompositeKey.class);
			// TODO Auto-generated constructor stub
		}
	@Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        /*
         * TODO one compareBytes call with all the bytes is enough
         */
    	//System.out.println("here2");
        int c = WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
        if (c != 0) {
            return c;
        }
        else {
        	int c2=WritableComparator.compareBytes(b1, s1 + 4, 4, b2, s2 + 4, 4);
        	if(c2!=0)
        		return c2;
        	return WritableComparator.compareBytes(b1, s1 + 8, 4, b2, s2 + 8, 4);
        }          
    }
	}
	static { // register this comparator
        WritableComparator.define(CompositeKey.class, new Comparator());
    }
	public CompositeKey(){
		
	}
	public CompositeKey(int id,int len,int rid){
		this.parID=id;
		this.length=len;
		this.RID=rid;
	}
	public int getID(){
		return parID;
	}
	public int getLength(){
		return length;
	}
	public int getRID(){
		return RID;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		parID=in.readInt();
		length=in.readInt();
		RID=in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(parID);
		out.writeInt(length);
		out.writeInt(RID);
	}

	@Override
	public int compareTo(CompositeKey key) {
		// TODO Auto-generated method stub
		//int value=0;
		//System.out.println("here1");
		int res=parID-key.parID;		
		if(res!=0)
			return res;
		if(res==0){
			int res2=length-key.length;
			if(res2!=0)
				return res2;
			else{
				int res3=RID-key.RID;
				if(res3!=0)
					return res3;
				else
					return 0;
			}
		}		
		return 0;
	}
	@Override
	public String toString() {
		return String.valueOf(parID)+","+String.valueOf(length)+","+String.valueOf(RID);
	}
}
