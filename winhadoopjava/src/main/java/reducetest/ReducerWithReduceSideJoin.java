package reducetest;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text, TaggedKey, Text>{
	private Text outvalue = new Text();
	@Override
	protected void reduce(TaggedKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		//Iterator: 반복자
		Iterator<Text> iterator = values.iterator();
		Text carrierName = new Text(iterator.next());	//항공사 이름
		while(iterator.hasNext()) {
			Text record = iterator.next();
			outvalue = new Text(carrierName.toString() + "\t" + record.toString());
			context.write(key, outvalue);
		}
	}
	
}
