package fs.generateprefixfile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;



public class prefixKeyPartitioner  extends Partitioner<prefixKey,Text> {

	@Override
	public int getPartition(prefixKey key, Text value, int partitionNumber) {
		// TODO Auto-generated method stub
		return key.getLength()%partitionNumber;
		//return 0;
	}
}
