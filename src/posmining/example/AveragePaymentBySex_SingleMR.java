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
 * 単一MRでの処理（Reducerでがんばるパターン）
 *
 * @author shin
 *
 */
public class AveragePaymentBySex_SingleMR {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(AveragePaymentBySex_SingleMR.class);   // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015901");                               // ★自分の学籍番号

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
		String outputpath = "out/averagePaymentBySex_SingleMR";   // ★MRの出力先
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
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// valueとなる売り上げ額を計算する
			int price = Integer.parseInt(csv[PosUtils.ITEM_COUNT]) * Integer.parseInt(csv[PosUtils.ITEM_PRICE]);

			// {key=sex, value="receiptId,price"} の形でemitする
			String sex = csv[PosUtils.BUYER_SEX];
			String rid_price = csv[PosUtils.RECEIPT_ID] + "," + price;
			context.write(new CSKV(sex), new CSKV(rid_price));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 平均を計算するためのHashMap
			// データ構造
			//     { {key:12345, value:[480, 980, 230, 105, ･･･]}
			//       {key:33333, value:[980, 105, 105, 105, ･･･]} }
			Map<String, List<Integer>> detailMap = new HashMap<>();

			// "receiptId,price"のリストを処理する
			for (CSKV value : values) {
				String v[] = value.toString().split(",");
				String receiptId = v[0];
				String price = v[1];

				// 始めて出てきたreceiptIdの場合，リストがnullなので新規リストを登録する
				if (detailMap.get(receiptId) == null) {
					detailMap.put(receiptId, new ArrayList<Integer>());
				}

				// リストに価格を追加する
				List<Integer> inner = detailMap.get(receiptId);
				inner.add(Integer.parseInt(price));
			}

			// レシート毎の合計金額をカウントするリスト
			List<Integer> prices = new ArrayList<>();

			// 全レシートの合計金額を確保
			for (String receiptId : detailMap.keySet()) {
				int total = 0;
				for (Integer price : detailMap.get(receiptId)) {
					total += price;
				}
				prices.add(total);
			}

			// レシート毎の合計金額をさらに合計
			int total = 0;
			for (Integer price : prices) {
				total += price;
			}

			// 男女別の平均金額
			double average = 1.0 * total / prices.size();

			// emit
			context.write(key, new CSKV(average));
		}
	}
}
