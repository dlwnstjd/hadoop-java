package dataexpo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
//복합키 클래스에서 group을 위한 설정.
public class TaggedGroupKeyComparator extends WritableComparator{

	protected TaggedGroupKeyComparator() {
		super(TaggedKey.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TaggedKey k1 = (TaggedKey) w1;
		TaggedKey k2 = (TaggedKey) w2;
		return k1.getCarrierCode().compareTo(k2.getCarrierCode());
	}
	
}
