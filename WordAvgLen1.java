package comp9313.ass1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAvgLen1 {
	// this is a Writable Pair called IntPair
	// referring to lecture notes ch3 p32-p33
	public static class IntPair implements Writable{
		private int first, second;
		
		public IntPair() {
		}
		public IntPair(int first, int second) {
			set(first, second);
		}
		public void set(int left, int right) {
			first = left;
			second = right;
		}
		public int getFirst(){
			return first;
		}
		public int getSecond() {
			return second;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second	);
		}
	}
	//mapper part
	public static class MyMapper 
		extends Mapper<Object, Text, Text, IntPair>{

		//store the sum of length and total number of word that start with particular letter.  
		private IntPair ip = new IntPair();
    
	    //store the first character of each word within a-z
	    private Text character = new Text();

	    public void map(Object key, Text value, Context context
	    		) throws IOException, InterruptedException {
    	
	    	//split the sentence into words and get each word
	    	StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
	    	while (itr.hasMoreTokens()) {
	    		String word = itr.nextToken().toLowerCase();
	    		
	    		/* check if first character within a~z, if so
	    		 * get the first character of word as key
	    		 * set IntPair, the first integer is length of word
	    		 * the second integer is 1
	    		 * the IntPair is the value
	    		 * emit the key and value 
	    		 */
	    		if (word.charAt(0) >= 'a' && word.charAt(0) <= 'z') {
	    			character.set(word.substring(0, 1));
	    			ip.set(word.length(), 1);
	    			context.write(character, ip);
	    		}
	    	}
	    }
	}
	
	//combiner part
	public static class MyCombiner
		extends Reducer<Text, IntPair, Text, IntPair> {
		
		IntPair ip = new IntPair();
		
		public void reduce(Text key, Iterable<IntPair> values, Context context
				) throws IOException, InterruptedException {
			// len to store the sum of the length that start with a particular letter
			int len = 0;
			// num to store the number of words that start with a particular letter
			int num = 0;
			
			/* read each IntPair with same key
			 * combine the value in IntPair
			 * sum of first integer is stored in 'len'
			 * sum of second integer is stored in 'num'
			 * use 'len' and 'num' to get new IntPair which is the value
			 * emit the key(not change) and value
			 */
			for (IntPair val : values) {
		    	len += val.getFirst();
		    	num += val.getSecond();
		    }
			ip.set(len, num);
			context.write(key, ip);
		}
	}
	
	//reducer part
	public static class MyReducer 
		extends Reducer<Text,IntPair,Text,DoubleWritable> {
		
		// store the calculation outcome.
		private DoubleWritable result = new DoubleWritable();
    	
		public void reduce(Text key, Iterable<IntPair> values, Context context
				) throws IOException, InterruptedException {
			// len to store the sum of the length that start with a particular letter
			double len = 0;
			// num to store number of words that start with a particular letter
		    int num = 0;
		    
		    /* read key and value, the key is a letter
		     * the value is a IntPair, get two integers from IntPair.
		     * sum the length of word that start with a particular letter with first integer
		     * count the number of word that start with a particular letter with second integer
		     * calculate the average length and store in result.
		     * emit the letter and result
		     */
		    for (IntPair val : values) {
		    	len += val.getFirst();
		    	num += val.getSecond();
		    }
		    double r = len / num;
		    result.set(r);
		    context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "wordavglen 1");
	    job.setJarByClass(WordAvgLen1.class);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyCombiner.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntPair.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
