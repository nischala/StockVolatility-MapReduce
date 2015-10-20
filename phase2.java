package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class phase2{
	public static class Map2 extends Mapper<Object, Text, Text, Text> {
		private Text key3 = new Text();
		private Text value3 = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String string=value.toString();
			String name[]=string.split("\t");
			String element[] = name[0].split(",");
			key3.set(element[0]);
			value3.set(name[1]);
			context.write(key3,value3);
			
			}
	}
	
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		private Text key4=new Text();
		private Text value4=new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			List<Double> list = new ArrayList<Double>();
			int N=0;
			double mean=0;
			double sum=0;
			double volatility=0;
			
			
			for(Text value:values) {
		    Double a=Double.parseDouble(value.toString());
		    list.add(a);
		    N++;
			}
			for(int i=0;i<N;i++) {
				mean=(mean+list.get(i)/N);
			}
			for(int j=0;j<N;j++) {
				double diff=list.get(j)-mean;
				double sqr=Math.pow(diff, 2);
				sum=sum+sqr;
			}
			volatility=Math.sqrt(sum/(N-1));
			key4.set(key.toString());
			value4.set(String.valueOf(volatility));
			if(volatility>0) {
				context.write(key4,value4);
			}
			
		}
	}
		
}