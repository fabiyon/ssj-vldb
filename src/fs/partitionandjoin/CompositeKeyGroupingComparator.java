package fs.partitionandjoin;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class CompositeKeyGroupingComparator extends WritableComparator {

	public CompositeKeyGroupingComparator(){
		super(CompositeKey.class,true);
	}
	@Override
	 public int compare(WritableComparable wc1, WritableComparable wc2) {
		 CompositeKey key1=(CompositeKey)wc1;
		 CompositeKey key2=(CompositeKey)wc2;
		 return key1.getID()-key2.getID();
	 }
}
