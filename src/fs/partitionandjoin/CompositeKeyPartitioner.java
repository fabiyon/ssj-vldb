package fs.partitionandjoin;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class CompositeKeyPartitioner  extends Partitioner<CompositeKey,Text> {

	@Override
	public int getPartition(CompositeKey key, Text value, int partitionNumber) {
		// TODO Auto-generated method stub
		return Math.abs(key.getID()%partitionNumber);
		//return 0;
	}

}
