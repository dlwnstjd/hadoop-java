package dataexpo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
//하둡의 Mapper에서 키로 사용할 클래스.
public class DateKey implements WritableComparable<DateKey>{
	private String year;
	private Integer month;
	public DateKey() {}
	public DateKey(String year, Integer date) {
		this.year = year;
		this.month = date;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public Integer getMonth() {
		return month;
	}
	public void setMonth(Integer month) {
		this.month = month;
	}	
	//StringBuilder, StringBuffer: 동적문자열 구현 클래스
	@Override
	public String toString() {
		return (new StringBuilder()).append(year).append(",").append(month).toString();
		//return year + "," + month; 성능때문에 사용안함
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		year = WritableUtils.readString(in);
		month = in.readInt();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, year);
		out.writeInt(month);
	}
	@Override
	public int compareTo(DateKey key) {
		int result = year.compareTo(key.year);
		if(0 == result) {	//year 값이 동일한 경우
			result = month.compareTo(key.month);
		}
		return result;
	}
	
}
