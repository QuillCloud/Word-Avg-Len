package comp9313.ass1;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.Set;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAvgLen2 {
	/* a class SimplePair help with store two integer
	 * use the first integer as sum of length of word that start with a particular letter
	 * use the second integer as total number of word that start with a particular letter
	 */
	public static class SimplePair {
	  final int x;
	  final int y;
	  SimplePair(int x, int y) {this.x=x;this.y=y;}
	  int getFirst() {return this.x;}
	  int getSecond() {return this.y;}
	}
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
		
		//store the total number and sum of length of word that start with a particular letter
		private IntPair len_num = new IntPair();
		
		//store the first character of each word within a-z
		private Text character = new Text();
		
		// HashMap key is letter, value is SimplePair
		// SimplePair store the sum of length and total number of word that start with that key value
		private HashMap<String, SimplePair> maps = new HashMap<String, SimplePair>();
 	
		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			
			//split the sentence into words and get each word
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			while (itr.hasMoreTokens()) {
				String word = itr.nextToken().toLowerCase();
				
				// get first character of word
				String c = word.substring(0, 1);
				
				// check if first character within a~z
				if (word.charAt(0) >= 'a' && word.charAt(0) <= 'z') {
					/* check if the hash map already has this letter as key
					 * 1. if not, use that letter as a new key, then
					 * use 1 and word's length to create a new SimplePair as value
					 * 2. if so,  update that key's value, the SimplePair
					 * add 1 to first integer and add word's length to second integer 
					 * then get the new SimplePair as the value.
					 */
					if (!maps.containsKey(word.substring(0, 1))) {
						SimplePair p = new SimplePair(word.length(), 1);
						maps.put(c, p);
					} else {
						SimplePair p = new SimplePair(word.length()
	 						+ maps.get(c).getFirst(), 1 + maps.get(c).getSecond());
						maps.put(c, p);
					}
				}
			}
		}
		
		// clean up part
		public void cleanup(Context context
				) throws IOException, InterruptedException {
			
			// read each entry in HashMap
			Set<Entry<String, SimplePair> > sets = maps.entrySet();
			/* for each entry, use entry's key as key that will be emit
			 * set the IntPair's value which are two integers in SimplePair 
			 * use this IntPair as value that will be emit
			 * emit the key-value
			 * the key is a letter in a-z
			 * the value is IntPair contains two integers
			 * first is total number of word that start with that key letter
			 * second is sum of length of word that start with that key letter
			 */
			for(Entry<String, SimplePair> entry : sets) {
				character.set(entry.getKey());
				len_num.set(entry.getValue().getFirst(), entry.getValue().getSecond());
				context.write(character, len_num);
			}
			maps.clear();
		}
	}
	
	//reducer part
	public static class MyReducer
	    extends Reducer<Text,IntPair,Text,DoubleWritable> {
		
		// store the calculation outcome.
		private DoubleWritable result = new DoubleWritable();
		 	
		public void reduce(Text key, Iterable<IntPair> values,Context context
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
    	                 * emit the the letter and result
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
		 Job job = Job.getInstance(conf, "wordavglen 2");
		 job.setJarByClass(WordAvgLen2.class);
		 job.setMapperClass(MyMapper.class);
		 job.setReducerClass(MyReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntPair.class);
		 job.setOutputValueClass(DoubleWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
