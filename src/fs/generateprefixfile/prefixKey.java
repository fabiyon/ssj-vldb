package fs.generateprefixfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class prefixKey implements  WritableComparable<prefixKey> {
	private int length;
	private int rid;	
	public prefixKey(){
		
	}
	public prefixKey(int len,int rid){
		this.length=len;
		this.rid=rid;
	}
	public int getLength(){
		return this.length;
	}
	public int getRID(){
		return this.rid;
	}
	public void setLength(int len){
		this.length=len;
	}
	public void setRID(int rid){
		this.rid=rid;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.length=in.readInt();
		this.rid=in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(length);
		out.writeInt(rid);		
	}
	@Override
	public int compareTo(prefixKey Object) {
		// TODO Auto-generated method stub
		int res=this.length-Object.length;
		if(res!=0)
			return res;
		else
			return this.rid-Object.rid;		
	}
	@Override
	public String toString(){
		return rid+" "+length;
	}
}
