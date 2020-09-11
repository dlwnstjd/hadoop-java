package dataexpo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
//사용자가 생성한 클래스를 하둡의 Key로 사용하기 위해서는 WritableComparable 인터페이스를 구현해야함 => key로 정렬됨.
public class TaggedKey implements WritableComparable<TaggedKey>{
	private String carrierCode;
	private Integer tag;
	public TaggedKey() {}
	public TaggedKey(String carrierCode, int tag) {
		this.carrierCode = carrierCode;
		this.tag = tag;
	}
	
	public String getCarrierCode() {
		return carrierCode;
	}
	public void setCarrierCode(String carrierCode) {
		this.carrierCode = carrierCode;
	}
	public Integer getTag() {
		return tag;
	}
	public void setTag(Integer tag) {
		this.tag = tag;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		carrierCode = WritableUtils.readString(in);
		tag = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, carrierCode);
		out.writeInt(tag);
	}

	@Override
	public int compareTo(TaggedKey key) {
		int result = this.carrierCode.compareTo(key.carrierCode);
		if(result == 0 ) {
			return this.tag.compareTo(key.tag);
		}
		return result;
	}

}
