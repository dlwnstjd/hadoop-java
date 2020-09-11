package reducetest;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import dataexpo.TaggedKey;

public class TaggedKeyComparator extends WritableComparator{
	protected TaggedKeyComparator() {
		super(TaggedKey.class, true);
	}
	@SuppressWarnings("rawtypes")	//warning 표시 금지
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TaggedKey k1 = (TaggedKey) w1;
		TaggedKey k2 = (TaggedKey) w2;
		int cmp = k1.getCarrierCode().compareTo(k2.getCarrierCode());
		if(cmp != 0) return cmp;
		return k1.getTag().compareTo(k2.getTag()) * (-1);
	}
	
	
}
