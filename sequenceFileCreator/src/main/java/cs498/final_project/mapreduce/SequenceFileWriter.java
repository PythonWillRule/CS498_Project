package cs498.final_project.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * The entry point for the Sequence Writer App example, which setup the Hadoop
 * job with MapReduce Classes
 * 
 * @author berman
 *
 */
public class SequenceFileWriter extends Configured implements Tool {
	/**
	 * Main function which calls the run method and passes the args using
	 * ToolRunner
	 * 
	 * @param args
	 *            Two arguments input and output file paths
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SequenceFileWriter(), args);
		System.exit(exitCode);
	}

	/**
	 * Run method which schedules the Hadoop Job
	 * 
	 * @param args
	 *            Arguments passed in main function
	 */
	public int run(String[] args) throws Exception {
		 Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);    
		 if (job == null) {                                                  
		   return -1;                                                        
		 }                                                                   
		                                                                     
		 job.setInputFormatClass(WholeFileInputFormat.class);                
		 job.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);           
		                                                                     
		 job.setOutputKeyClass(BytesWritable.class);                                  
		 job.setOutputValueClass(BytesWritable.class);                       
		                                                                     
		 job.setMapperClass(SequenceFileMapper.class);                       
		                                                                     
		 return job.waitForCompletion(true) ? 0 : 1;                         
	}

	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, BytesWritable, BytesWritable> {

		private BytesWritable filenameKey;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			filenameKey = new BytesWritable(path.toString().getBytes());
		}

		@Override
		protected void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}

	}

}
