package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.commoncrawl.warc.WARCFileInputFormat;

/**
 * HTML tag count example using the raw HTTP responses (WARC) from the Common Crawl dataset.
 *
 * @author Stephen Merity (Smerity)
 */
public class WARCTagCounter extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WARCTagCounter.class);

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop job.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WARCTagCounter(), args);
		System.exit(res);
	}

	/**
	 * Builds and runs the Hadoop job.
	 * 
	 * @param args command line arguments
	 * @return 0 if the Hadoop job completes successfully and 1 otherwise.
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: " + this.getClass().getSimpleName() + " <outputpath> <inputpath>...");
			return -1;
		}
		Path outputPath = null;
		List<Path> inputPaths = new ArrayList<>();
		for (int i = 0; i < args.length; i++) {
			if (outputPath == null) {
				outputPath = new Path(args[i]);
			} else {
				inputPaths.add(new Path(args[i]));
			}
		}
		return run(outputPath, inputPaths.toArray(new Path[inputPaths.size()]));
	}

	public int run(Path outputPath, Path[] inputPaths)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf);
		job.setJarByClass(WARCTagCounter.class);
		job.setNumReduceTasks(1);

		for (int i = 0; i < inputPaths.length; i++) {
			LOG.info("Input path: " + inputPaths[i]);
			FileInputFormat.addInputPath(job, inputPaths[i]);
		}

		LOG.info("Output path: " + outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(TagCounterMap.TagCounterMapper.class);
		job.setReducerClass(LongSumReducer.class);

		return job.waitForCompletion(true) ? 0 : -1;
	}
}
