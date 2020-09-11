package dataexpo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/*
 * 두개의 파일을 Mapper을 이용하여 Join하기
 */
public class MapSideJoin {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String arg[] = {
				"C:/ubuntu_share/dataexpo/carriers.csv",
				"C:/ubuntu_share/dataexpo/1988.csv",
				"outfile/mapsidejoin"
		};
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(new Path(arg[2]))) {
			System.out.println("기존 출력파일 삭제");
			hdfs.delete(new Path(arg[2]));
		}
		Job job = new Job(conf, "MapSideJoin");
		//cache 파일로 저장: carriers.csv 파일 정보를 cache에 저장함.
		//Mapper에서 사용할 수 있도록
		job.addCacheFile(new Path(arg[0]).toUri());
		//입력파일 설정
		FileInputFormat.addInputPath(job, new Path(arg[1]));
		//출력파일 설정
		FileOutputFormat.setOutputPath(job, new Path(arg[2]));
		//작업 클래스 설정
		job.setJarByClass(MapSideJoin.class);
		//Mapper클래스 설정
		job.setMapperClass(MapperWithMapSideJoin.class);
		//Reducer 클래스는 0개다.(없음)
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
