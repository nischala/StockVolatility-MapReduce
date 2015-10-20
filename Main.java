package mapreduce;


import java.util.Date;


//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

public class Main {

	public static void main(String[] args) throws Exception {		
		long start = new Date().getTime();		
		//Configuration conf = new Configuration();
		
		
	     Job job = Job.getInstance();
	     job.setJarByClass(phase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(phase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(phase3.class);
		 

		System.out.println("\n**********Start**********\n");

		job.setJarByClass(phase1.class);
		job.setMapperClass(phase1.Map1.class);
		job.setReducerClass(phase1.Reduce1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job2.setJarByClass(phase2.class);
		job2.setMapperClass(phase2.Map2.class);
		job2.setReducerClass(phase2.Reduce2.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		
		job3.setJarByClass(phase3.class);
		job3.setMapperClass(phase3.Map3.class);
		job3.setReducerClass(phase3.Reduce3.class);

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		//int NOfReducer3 = 1; 
		job3.setNumReduceTasks(1);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path("Inter_"+args[1]));
		FileInputFormat.addInputPath(job2, new Path("Inter_"+args[1]));
		FileOutputFormat.setOutputPath(job2, new Path("Inter2_"+args[1]));
		FileInputFormat.addInputPath(job3, new Path("Inter2_"+args[1]));
		FileOutputFormat.setOutputPath(job3, new Path("Final"+args[1]));
		
		job.waitForCompletion(true);

		job2.waitForCompletion(true);
		boolean status = job3.waitForCompletion(true);
		if (status == true) {
			long end = new Date().getTime();
		
			System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
		}
		System.out.println("\n**********End**********\n");		
	}
}

