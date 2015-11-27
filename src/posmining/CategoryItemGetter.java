package posmining;

import java.io.IOException;

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
 * 入力されたカテゴリの商品名に対応する売り上げと個数のリストを出力する
 * @author kengo92i
 *
 */
public class CategoryItemGetter {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String jobname = "2015026";
		Class<CategoryItemGetter> jarclass = CategoryItemGetter.class;
		String outdir = "CategoryItemGetter";
		String[] category_list = {
				"チルドムース",
				"フルーツ入りヨーグルト",
				"チルドプリン",
				"ドリンクヨーグルト",
				"半生ケーキ",
				"常温ゼリー",
				"チョコレート菓子",
				};

		String input = args.length > 0 ? args[0] : "posdata";
		for(String category : category_list){
			String output = String.format("out/%s/%s", outdir, category);
			runJobWithParam(category, input, output, jobname, jarclass);
		}
	}

	private static void runJobWithParam(String category, String input, String output, String jobname, Class<?> jarclass)
			 throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Configuration conf = new Configuration();
		conf.set("category", category);
		Job job = new Job(conf);

		job.setJarByClass(jarclass);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName(jobname);

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(output);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);
		// MapReduceジョブを投げ，終わるまで待つ．

		// waitForだと前のジョブが終わってから次を投入するので，途中でeclipseを終了すると以降のジョブが走らない
		// submitだと何も待たずに全部突っ込むのでeclipse関係ない
		// ただしローカル環境でsubmitするとOutOfMemoryなので，ローカルならwaitFor, EMRならsubmitにすればいい
		//job.waitForCompletion(true); // 待つ
		job.submit();  // 待たない
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// ターゲットカテゴリでないレシートは無視
			Configuration conf = context.getConfiguration();
			if (csv[PosUtils.ITEM_CATEGORY_NAME].equals(conf.get("category")) == false) {
				return;
			}

			// valueとなる商品名を取得
			String name = csv[PosUtils.ITEM_NAME];
			String sales = csv[PosUtils.ITEM_COUNT] + ":" +  csv[PosUtils.ITEM_TOTAL_PRICE];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(name), new CSKV(sales));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			// 合計を計算
			int total_count = 0;
			long total_price = 0;  // オーバフロー大丈夫か？単価を出力しといてexcelでトータルのがいいかな
			for (CSKV value : values) {
				String sales[] = value.toString().split(":");
				total_count += Integer.parseInt(sales[0]);
				total_price += Integer.parseInt(sales[1]);
			}

			// emit
			String out = String.valueOf(total_count) + "\t" + String.valueOf(total_price);
			context.write(key, new CSKV(out));
		}
	}
}
