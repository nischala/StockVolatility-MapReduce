package mapreduce;


import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class phase1 {
	
	public static class Map1 extends Mapper<Object, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();
		//private Text value2 = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			if(line.charAt(0)!='D') {
			FileSplit file=(FileSplit)context.getInputSplit();
			String filename = file.getPath().getName();
			String element[] = null;
			element = line.split(",");
			String date[] = null;
			date = element[0].split("-");
			key1.set(filename+","+date[0]+","+date[1]);
			value1.set(date[2]+","+element[6]);
			context.write(key1, value1);
			
		

			}
		}
	}



	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		private Text key2=new Text();
		private Text value2=new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			int firstDay=32;
			int lastDay=0;
			double xi=0;
			
			for(Text value:values){
				String line = value.toString();
				String day[]=line.split(",");
				map.put(Integer.parseInt(day[0]),Double.parseDouble(day[1]));
			}
			
			
			for(int day:map.keySet()){
				if(lastDay<day)
			     lastDay=day; 
			}
			  
			for(int day:map.keySet()){
				if(firstDay>day)
			     firstDay=day; 
			}
			xi=(map.get(lastDay)-map.get(firstDay))/map.get(firstDay);
			key2.set(key.toString());
			value2.set(String.valueOf(xi));
			context.write(key2,value2);
			
			
		}
	}
	
}
