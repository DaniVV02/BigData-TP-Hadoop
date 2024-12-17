
import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupBy {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupBy.class.getName());

	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

		try {
			FileHandler fh = new FileHandler("out.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		} catch (SecurityException | IOException e) {
			System.exit(1);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			if (key.get() == 0) return;

			String line = value.toString();
			String[] columns = line.split(",");

			if (columns.length > 20) {
				String orderDate = columns[2];    // Column 3: Order Date
				String state = columns[10];      // Column 11: State
				String category = columns[14];   // Column 15: Category
				String orderId = columns[1];     // Column 2: Order ID
				String productId = columns[13];  // Column 16: Product ID
				String salesStr = columns[17];   // Column 19: Sales
				String quantityStr = columns[18];// Column 20: Quantity

				try {
					// sales by (Date, State)
					double sales = Double.parseDouble(salesStr);
					context.write(new Text("DateState: " + orderDate + ", " + state), new Text("sales:" + sales));

					// sales by (Date, Category)
					context.write(new Text("DateCategory: " + orderDate + ", " + category), new Text("sales:" + sales));

					// product and quantity by Order ID for product statistics
					context.write(new Text("Order:" + orderId), new Text("product:" + productId));
					context.write(new Text("Order:" + orderId), new Text("quantity:" + quantityStr));
				} catch (NumberFormatException e) {
					LOG.warning("Invalid sales/quantity value: " + salesStr + " / " + quantityStr);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String keyStr = key.toString();
			if (keyStr.startsWith("DateState:") || keyStr.startsWith("DateCategory:")) {
				// ici on handle les sales aggregation
				double totalSales = 0.0;
				for (Text value : values) {
					String[] parts = value.toString().split(":");
					if (parts[0].equals("sales")) {
						totalSales += Double.parseDouble(parts[1]);
					}
				}
				context.write(key, new Text("TotalSales: " + totalSales));
			} else if (keyStr.startsWith("Order:")) {
				// ici on handle les order statistics
				Set<String> uniqueProducts = new HashSet<>();
				int totalQuantity = 0;

				for (Text value : values) {
					String[] parts = value.toString().split(":");
					if (parts[0].equals("product")) {
						uniqueProducts.add(parts[1]);
					} else if (parts[0].equals("quantity")) {
						totalQuantity += Integer.parseInt(parts[1]);
					}
				}
				context.write(key, new Text("distinctProducts:" + uniqueProducts.size() + ",totalQuantity:" + totalQuantity));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupByExtended");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		//job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}