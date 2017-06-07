/*----------------------------------------------------------------
 *  Author:        Konidaris Vissarion
 *  Written:       16/5/2017
 *  Last updated:  1/6/2017
 *
 *  Execution:     hadoop jar JarName.jar input_file_1X.csv OutputFileName.csv NumberOfPartions random/angle Dimensions
 *  
 * 	Skyline Queries with Hadoop Map-Reduce
 *
 *----------------------------------------------------------------*/

package tuc.softnet.hadoop.mapreduce.skyline_queries;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SkylineMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
	private Text word = new Text();
	private Random rnd = new Random();
	private static int Splits;
	private static String parTech;
	private int Dimensions;
	private String Bounds;
	
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> collector, Reporter reporter)
	throws IOException {
		String line = value.toString(); //read tuple
		if ( line.charAt(0)>='0' && line.charAt(0)<='9' && !line.isEmpty() && Splits>1){
			// If tuple is not the header, then call the partition function for setting the integer key and pass the tuple as text value
			word.set(line);
			collector.collect(new IntWritable(partition(line)), word);
		}
		else{
			// Pass the header with key 0
			word.set(line);
			collector.collect(new IntWritable(0), word);
		}
	}
	
	public int partition(String line){
		if(parTech.equals("random")){
			// In case of random partitioning, choose an int key between [0,partitions-1]
			return rnd.nextInt(Splits);
		}else{
			// Case of angle based partitioning
			String[] bounds=Bounds.split(",");
			String[] splits=line.split(",");
			///////////////////////// Partition the tuple using the bounds passed to the mapper ////////////////////////
			if(Dimensions==2){ // Using 2 dimensions
				int count=0;
				Double f1=Math.atan((Double.parseDouble(splits[2])/Double.parseDouble(splits[1])));
				for(String split:bounds){
					if(f1<=Double.parseDouble(split)){
						return count;
					}
					count++;
				}
				return count;
			}else{ // Using 3 dimensions
				Double f1=Math.atan(Math.sqrt(Math.pow(Double.parseDouble(splits[3]),2)+Math.pow(Double.parseDouble(splits[2]),2))/Double.parseDouble(splits[1]));
				Double f2=Math.atan((Double.parseDouble(splits[3])/Double.parseDouble(splits[2])));
				int i,j;
				for(i=0;i<2;i++){
					if(f1<=Double.parseDouble(bounds[i])){
						break;
					}
				}
				for(j=0;j<2;j++){
					if(f2<=Double.parseDouble(bounds[2+j])){
						return i+3*j;
					}
				}
				return i+3*j;
			}
			///////////////////////////////////////////////////////////////////////////////////////////////////////////
		}
	}
	
	 // Reading the arguments of the MapReduce Job
	 public void configure(JobConf job) {
		 Splits = (int)Long.parseLong(job.get("splits"));
		 parTech = job.get("partitioningTechnique");
		 Dimensions = (int)Long.parseLong(job.get("dimensions"));
		 Bounds = job.get("bounds");
	 }

}