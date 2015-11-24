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
 * 入力されたカテゴリの商品名リストを性別/年齢層別に出力する
 * @author tanaka-h
 *
 */
public class CategoryItemGetterByAgeAndSex {


	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(CategoryItemGetterByMonths.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2015021");                        // ★自分の学籍番号

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
		String outputpath = "out/categoryItemGetterByAgeAndSex";     // ★MRの出力先
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

			// チルドプリンでないレシートは無視
			if (csv[PosUtils.ITEM_CATEGORY_NAME].equals("チルドプリン") == false) {
				return;
			}

			// keyとなる商品名，年齢,性別を作る
			String sex = csv[PosUtils.BUYER_SEX];
			String name = csv[PosUtils.ITEM_NAME];
			String age = csv[PosUtils.BUYER_AGE];

			// valueとなる個数，価格を取得
			String count = csv[PosUtils.ITEM_COUNT];
			String price = csv[PosUtils.ITEM_TOTAL_PRICE];

			//購入者性別フラグ:1=男性,2=女性
			if(sex.equals("1")){sex = "男性";}else{sex = "女性";}

			//購入者年齢フラグ:1=子供,2=若者,3=大人,4=実年
			switch(age){
			case("1"):age = "子供"; break;
			case("2"):age = "若者"; break;
			case("3"):age = "大人"; break;
			case("4"):age = "実年"; break;
			}

			//emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new CSKV(name +"\t"+ sex + "\t" + age),new CSKV(count+","+price));
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
			context.write(key, new CSKV(total_count+"個"+"\t"+"計"+total_price+"円"));
		}
	}
}
