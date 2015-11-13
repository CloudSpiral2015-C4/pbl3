package posmining.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

/**
 * 男女別の平均使用金額を計算する
 * 単一MRでの処理（Mapperでがんばるパターン，In-mapper combinerパターン）
 *
 * @author shin
 *
 */
public class AveragePaymentBySex_InMapper {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(AveragePaymentBySex_InMapper.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015901");                        // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// 入出力ファイルを指定
		String inputpath = "posdata";
		String outputpath = "out/averagePaymentBySex_InMapper";    // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}
		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

		// 平均を計算するためのHashMap
		// データ構造
		//     { {key:"12345,男", value:[98, 98, 105, ･･･]}
		//       {key:"33333,女", value:[320, 98, 98, ･･･]} }
		private Map<String, List<Integer>> detailMap = new HashMap<>();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// valueとなる売り上げ額を計算する
			int price = Integer.parseInt(csv[PosUtils.ITEM_COUNT]) * Integer.parseInt(csv[PosUtils.ITEM_PRICE]);

			String rid_sex = csv[PosUtils.RECEIPT_ID] + "," + csv[PosUtils.BUYER_SEX];

			if (detailMap.get(rid_sex) == null) {
				detailMap.put(rid_sex, new ArrayList<Integer>());
			}
			detailMap.get(rid_sex).add(price);
		}

		// In-mapper combiner
		// Mapで処理した前キーをまとめて処理する
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (String k : detailMap.keySet()) {
				String v[] = k.split(",");
				Integer sex = Integer.parseInt(v[1]);

				long total = 0;
				for (Integer price : detailMap.get(k)) {
					total += price;
				}
				context.write(new CSKV(sex), new CSKV(total));
			}
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {
			long total = 0, n = 0;
			for (CSKV value : values) {
				total += value.toInt();
				n++;
			}
			double average = 1.0 * total / n;
			// emit
			context.write(key, new CSKV(average));
		}
	}
}
