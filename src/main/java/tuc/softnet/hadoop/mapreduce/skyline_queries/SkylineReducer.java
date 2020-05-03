/*----------------------------------------------------------------
 *  Author:        Xinyu Li
 *  Written:       4/10/2020
 *  Last updated:  4/20/2020
 *
 *  Execution:     hadoop jar JarName.jar input_file_1X.csv OutputFileName.csv NumberOfPartions random/angle Dimensions
 *  
 * 	Skyline Queries with Hadoop Map-Reduce
 *
 *----------------------------------------------------------------*/

package tuc.softnet.hadoop.mapreduce.skyline_queries;

import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SkylineReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text,Text>{
	private Text Id = new Text();
	private Text Specs = new Text();
	private String Data;
	private static int Dimensions;
	
	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, 
			Reporter reporter) throws IOException {
		// Call BNL to compute the global skyline of the dataset
		for (ArrayList<Double> Apartment:BNL(values,outputCollector)) {
			// Parse each skyline point
			Data="";
			for(int i=1;i<Apartment.size();i++){
				Data+=" "+Apartment.get(i).toString();
				if(i<Apartment.size()-1)
					Data+=" ,";
			}
			// Write the resulting point to the output file of the job
			Id.set(Apartment.get(0).toString().substring(0, Apartment.get(0).toString().indexOf("."))+" ");
			Specs.set(Data);
			outputCollector.collect(Id,Specs);
		}
				
	}
	
	// The Block Nested Loop algorithm for calculating the skyline points
	// The algorithm also sorts the skyline points iteratively in ascending order.
	private ArrayList<ArrayList<Double>> BNL(Iterator<Text> values,OutputCollector<Text, Text> outputCollector) throws IOException{
		ArrayList<ArrayList<Double>> ApList = new ArrayList<ArrayList<Double>>();
		int sortingIndex;
		while (values.hasNext()) { // for each tuple
			
			// parse the datapoint
			ArrayList<Double> Apartment = new ArrayList<Double>();
			String[] splits = values.next().toString().split(",");
			try{
				Double.parseDouble(splits[0]);
			}
			catch (Exception e){
				// If the tuple is the header, send it to the outputCollector immediately
				String Header="";
				for (int i=1;i<splits.length;i++){
					Header+=splits[i];
					if(i<splits.length-1)
						Header+=" ,";
				}
				Id.set(splits[0]);
				Specs.set(Header);
				outputCollector.collect(Id,Specs);
				continue;
			}
			// If not parse its values
			for (String Data : splits)
				Apartment.add(Double.parseDouble(Data));
			
			// BNL
			if (ApList.isEmpty()){
				ApList.add(Apartment);
			}else{
				sortingIndex=0;
				for(int i=0;i<ApList.size();i++){
					int count=0;
					if(Apartment.get(0)>ApList.get(i).get(0))
						sortingIndex++;
					for(int j=1;j<=Dimensions;j++){
						if(Apartment.get(j)<ApList.get(i).get(j))
							count++;
					}
					if(count==0){
						break;
					}else{
						if(count==Dimensions){
							ApList.remove(i);
							if(sortingIndex>i)
								sortingIndex--;
							i--;
						}
						if(i==ApList.size()-1){
							ApList.add(sortingIndex, Apartment);
							i++;
						}
					}
				}
			}
		}
		return ApList;
	}

	// Reading the arguments of the MapReduce Job
	public void configure(JobConf job) {
		 Dimensions = (int)Long.parseLong(job.get("dimensions"));
	}
	
}
