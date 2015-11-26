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

import posmining.HourlySalesGetter.MyMapper;
import posmining.HourlySalesGetter.MyReducer;
import posmining.utils.CSKV;
import posmining.utils.PosUtils;

/**
 * 入力されたカテゴリの商品名リストを年齢層別に出力する
 * @author tanaka-h
 *
 */
public class CategoryItemGetterByAge {



	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String jobname = "2015021";
		Class<HourlySalesGetter> jarclass = HourlySalesGetter.class;
		String outdir = "Hourly";
		String[] target_list = {
				"1",
				"2",
				"3",
				"4"
				};
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
		for(String target : target_list){
			for(String category : category_list){
				String output = String.format("out/age/%s/%s/%s", outdir, target,category);
				runJobWithParam(category, target,input, output, jobname, jarclass);
			}
		}
	}

	private static void runJobWithParam(String category,String target, String input, String output, String jobname, Class<?> jarclass)
			 throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Configuration conf = new Configuration();
		conf.set("category", category);
		conf.set("target", target);
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
	//	job.waitForCompletion(true); // 待つ
		job.submit();  // 待たない
	}

	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split(",");

			// ターゲットカテゴリでないレシートは無視
			Configuration conf = context.getConfiguration();
			if (csv[PosUtils.ITEM_CATEGORY_NAME].equals(conf.get("category")) == false
					|| csv[PosUtils.BUYER_AGE].equals(conf.get("target")) == false
					) {
				return;
			}

			// keyとなる商品名，年齢を作る
			String name = csv[PosUtils.ITEM_NAME];
			String age = csv[PosUtils.BUYER_AGE];

			// valueとなる個数，価格を取得
			String count = csv[PosUtils.ITEM_COUNT];
			String price = csv[PosUtils.ITEM_TOTAL_PRICE];


			//購入者年齢フラグ:1=子供,2=若者,3=大人,4=実年
			switch(age){
			case("1"):age += "子供"; break;
			case("2"):age += "若者"; break;
			case("3"):age += "大人"; break;
			case("4"):age += "実年"; break;
			}

			//emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(name),new CSKV(count+","+price));
		}
	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {
		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			int total_count=0;
			long total_price=0;
			// 売り上げを合計
			for (CSKV value : values) {
				String v[] = value.toString().split(",");
				total_count += Integer.parseInt(v[0]);
				total_price += Integer.parseInt(v[1]);
			}


			// emit
			context.write(key, new CSKV(total_count+"\t"+total_price));
		}
	}
}
