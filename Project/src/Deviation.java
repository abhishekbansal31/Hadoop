import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Deviation {

	/*		 Mapper1 Class for upsldc data			*/
	
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, TextPair> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String clearRecord = value.toString().replaceAll("[\\[\\]''(){}]","");
	      
			StringTokenizer str = new StringTokenizer(clearRecord.toString(),",");
	      
			boolean goodRecord = validate(clearRecord);
			//boolean goodRecord = true;
		      
			if(goodRecord) {
	
			  String datetime = str.nextToken().substring(0,16);
		      // not concern about these
		      int schedule = Integer.valueOf(str.nextToken());
		      int drawl = Integer.valueOf(str.nextToken());
		      int odud = Integer.valueOf(str.nextToken());
		      
		      int demand = Integer.valueOf(str.nextToken());
		 
	   /*     int totalSSGS = Integer.valueOf(str.nextToken());
		      int thermal = Integer.valueOf(str.nextToken());
		      int ipp = Integer.valueOf(str.nextToken());
		      int hydro = Integer.valueOf(str.nextToken());
		      int cpp = Integer.valueOf(str.nextToken());
		      int solar = Integer.valueOf(str.nextToken());
		      int freq = Integer.valueOf(str.nextToken());
		      int dr = Integer.valueOf(str.nextToken());	*/

			  Text datetimeText = new Text(datetime);
		      IntWritable demandIntWritable = new IntWritable(demand);
		      context.write(datetimeText,new TextPair(demandIntWritable,new Text("0")));
	      }
	    }
	    
	    private boolean validate(String record) {

			StringTokenizer str = new StringTokenizer(record.toString(),",");
		    // if datetime is null then not a good record
	    	if(str.nextToken().equals("")) {
	    		return false;
	    	}
	    //	2nd 3rd and 3rd columns are not concerned
	    	str.nextToken();
	    	str.nextToken();
	    	str.nextToken();
	    // if demand is null then not a good record
	    	if(str.nextToken().equals("")) {
	    		return false;
	    	}
	    		    	
	    	return true;
	    }
	}

	/*		 Mapper2 Class for upsldc data			*/
	
	public static class Mapper2 extends Mapper<LongWritable , Text, Text, TextPair>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
		    String clearRecord = value.toString().replaceAll("[\\[\\]''(){}]","");
			StringTokenizer str = new StringTokenizer(clearRecord.toString(),",");

			boolean goodRecord = validate(clearRecord);
			//boolean goodRecord = true;
			if(goodRecord) {
				String datetime = str.nextToken().substring(0,16);
				
				Text datetimeText = new Text(datetime);
				int demand = Integer.valueOf(str.nextToken());
				
		/*		int ownGeneration = Integer.valueOf(str.nextToken());
				int importPower = Integer.valueOf(str.nextToken());   */
				
				IntWritable demandIntWritable = new IntWritable(demand);

			    context.write(datetimeText,new TextPair(demandIntWritable,new Text("1")));
				
			}
		}
		/* Return true if record is in correct format */ 
		private boolean validate(String record) {

			StringTokenizer str = new StringTokenizer(record.toString(),",");
		    // if datetime or demand is null then not a good record
    		if(str.nextToken().equals("") || str.nextToken().equals("")) {
    			return false;
    		}

	    	return true;
	    }
		
	}
 
  	/*            Partitioner Class                  */
	public static class MyPartitioner extends Partitioner<Text, TextPair>{
		
		@Override
		public int getPartition(Text key, TextPair value, int numReducerTasks) {
			
			return 0 ;
		
		}
	}

	  
	/*                 Reducer Class                  */
	public static class Reduce extends Reducer<Text,TextPair,Text,IntWritable> {

		@Override
	    public void reduce(Text key, Iterable<TextPair> values, Context context) 
	    		throws IOException, InterruptedException {
			
			/*for (TextPair val : values) {
    			context.write(key, new IntWritable(val.getFirst().get()));
    			context.write(key, new IntWritable(Integer.valueOf(val.getSecond().toString())));
	    	}*/
			
	    	int upsldcDemand = 0;
	    	int meritindiaDemand = 0;
	    	int deviation = 0;
	    //	Find value of demand from upsldc
	    	for (TextPair val : values) {
	    		if(val.getSecond().toString().equals("0")) {
	    			upsldcDemand = val.getFirst().get();
	    			break;
	    		}
	    	}
		    //	Find value of demand from meritindia
	    	for (TextPair val : values) {
	    		if(val.getSecond().toString().equals("1")) {
	    			meritindiaDemand = val.getFirst().get();
	    			break;
	    		}
	    	}
	    	//		Find deviation
	    	if(upsldcDemand!=0 && meritindiaDemand!=0) {
				    deviation = meritindiaDemand - upsldcDemand;
				    IntWritable deviationIntWritable = new IntWritable(deviation);
				    context.write(key, deviationIntWritable);
		    }
	    }
	}
	  
	
	/*              Driver Code                  */
	public static void main(String[] args) throws Exception {
		
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Deviation");
	    
	    job.setJarByClass(Deviation.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TextPair.class);
	  //  job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	    
	    job.setNumReduceTasks(1);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Mapper1.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper2.class);
	    
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


class TextPair implements WritableComparable<TextPair> {

	private IntWritable first;
	private Text second;
	
	public TextPair() {
		set(new IntWritable(), new Text());
	}
	
	public TextPair(IntWritable first, Text second) {
		set(first, second);
	}
	
	public void set(IntWritable first, Text second) {
		this.first = first;
		this.second = second;
	}
	
	public IntWritable getFirst() {
		return first;
	}
	
	public Text getSecond() {
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@Override
	public int compareTo(TextPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo(tp.second);
	}
	
}