package mapreduce;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class phase3{
	public static class Map3 extends Mapper<Object, Text, Text, Text> {
		private Text key5=new Text();
		private Text value5=new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String element[] = line.split("\t");
			
			key5.set("KEY");
			
			value5.set(element[0]+","+element[1]);
			context.write(key5, value5);
		}
	}
	
	
	
		
	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Double, String> Map = new TreeMap<Double, String>();
		//SortedMap<Double, Text> treemap = new TreeMap<Double, Text>();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				String line=value.toString();
				String key_file[]=line.split(",");
				Map.put(Double.parseDouble(key_file[1]),key_file[0]);
			}
			
			
			int counter=0;
			context.write(new Text("Lowest 10 Stocks"),new Text(" "));
			context.write(new Text("   "), new Text("      "));
		    for(double i:Map.keySet()) {
		    	counter++;
		    	if(counter>10)
		    		break;
		    	
		    	context.write(new Text(Map.get(i)), new Text(String.valueOf(i)));
		    	
		    }
		    counter=0;
		    context.write(new Text("   "), new Text("      "));
		    context.write(new Text("Highest 10 Stocks"),new Text(" "));
		    context.write(new Text("   "), new Text("      "));
		    for(double i:Map.descendingKeySet()) {
		    	counter++;
		    	if(counter>10)
		    		break;
		    	
		    	context.write(new Text(Map.get(i)), new Text(String.valueOf(i)));
		    }

		 }
      }
	}	
	
	
	
