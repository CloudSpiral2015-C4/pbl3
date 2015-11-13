package posmining.utils;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PosFormatter {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJobName("Posデータの整形");

		job.setJarByClass(PosFormatter.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// ハッシュで使うkey-valueの型（クラス）をテキストと整数に指定
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// 入出力ファイル
		String inputpath = "./posdata";
		String outputpath = "./out/formatted";

		PosUtils.deleteOutputDir(outputpath);

		// ファイルのパスを指定
		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// Reduceで使う計算機数を指定
		job.setNumReduceTasks(8);

		// ジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}

	// Mapperを継承して，map関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private int n = 0;
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			n++;
			String v[] = value.toString().split(",");
			// 非完全な行を無視する（具体的には商品名＝空がほとんど）
			if (v.length < PosUtils.N_ROWS) {
				System.out.println("Skipped an incomplete record: " + v.length + " in line " + n);
				return;
			}

			// 商品名の全角スペースやタブをつぶしておく
			v[PosUtils.ITEM_NAME] = v[PosUtils.ITEM_NAME].replaceAll("[　\\s]+", " ");

			// map
			context.write(new LongWritable(n), new Text(StringUtils.join(v, ",")));
		}
	}

	// Reducerを継承して，reduce関数を定義
	public static class MyReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// reduce
			context.write(key, values.iterator().next());
		}
	}
}
