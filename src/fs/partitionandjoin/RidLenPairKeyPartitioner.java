package fs.partitionandjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class RidLenPairKeyPartitioner extends Partitioner<RidLenPairKey,IntWritable> {
	@Override
	public int getPartition(RidLenPairKey key, IntWritable values, int partitionNumber) {
		// TODO Auto-generated method stub
		return Math.abs(key.toString().hashCode()%partitionNumber);
		//return 0;
	}

}
