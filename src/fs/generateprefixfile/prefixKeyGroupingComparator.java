package fs.generateprefixfile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class prefixKeyGroupingComparator extends WritableComparator{

	protected prefixKeyGroupingComparator(){			
		super(prefixKey.class,true);
		// TODO Auto-generated constructor stub
	}
	@Override
	 public int compare(WritableComparable wc1, WritableComparable wc2) {
		prefixKey key1=(prefixKey)wc1;
		prefixKey key2=(prefixKey)wc2;
		 return key1.getLength()-key2.getLength();
	 }

}
