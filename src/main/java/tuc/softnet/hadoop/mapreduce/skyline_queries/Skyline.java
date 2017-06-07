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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Skyline {

	public static void main(String[] args) throws Exception{
		 
		////////////////////////////// Checking for valid arguments  //////////////////////////
		// Check if the argument list is empty
		if (args==null){
			System.out.println("You need to pass arguments!");
			System.exit(0);
		}
		
		// Check if the number of partitions are between [1,10]
		if(!(Integer.parseInt(args[2])>=1 && Integer.parseInt(args[2])<=10)){
			System.out.println("Number of partitions must be in range [1,10]");
			System.exit(0);
		}
		
		// Check if number of partitions are 9 for angle partitioning technique using 3 dimensions
		if((args[3].equals("angle") && Integer.parseInt(args[4])==3)){
			if(Integer.parseInt(args[2])!=9){
				System.out.println("Angle Partitioning with 3 dimensions only accepts 9 partitions!");
				System.exit(0);
			}
		}
		///////////////////////////////////////////////////////////////////////////////////////
		
		Configuration conf = new Configuration(); // Hadoop configuration
		conf.set("mapred.textoutputformat.separator", ",");
		FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf); // Initialize the "distributed" file system.
		String Input=""; // Name of input file
		// Default bounds for the angled partition using 3 dimensions and 9 partitions
		String bounds=String.valueOf(48.24*Math.PI/180)+","+String.valueOf(70.52*Math.PI/180)+","+String.valueOf(30*Math.PI/180)+","+String.valueOf(60*Math.PI/180);

		
		// Check if /input directory exist in hadoop distributed file system (hdfs). If not it's created.
		if(!hdfs.exists(new Path("hdfs://127.0.0.1:9000/input/"))){
			System.out.println("Creating /input directory of hdfs");
			hdfs.mkdirs(new Path("hdfs://127.0.0.1:9000/input/"));
		}
		
		// Check if /output directory exist in hdfs. If not it's created.
		if(!hdfs.exists(new Path("hdfs://127.0.0.1:9000/output/"))){
			System.out.println("Creating /output directory of hdfs");
			hdfs.mkdirs(new Path("hdfs://127.0.0.1:9000/output/"));
		}
		
		// Parsing the name of the input file.
		for(int i=args[0].length()-1; i>-1;i--){
			if(args[0].charAt(i)!='\\'){
				Input=args[0].charAt(i)+Input;
			}else{
				break;
			}
		}
		System.out.println(Input);
		
		if(args[3].equals("random")||(args[3].equals("angle") && Integer.parseInt(args[4])==3)){
			// Check if input file is not in the hdfs. If not upload it.
			if(!hdfs.exists(new Path("hdfs://127.0.0.1:9000/input/"+Input))){
				//long tStart = System.currentTimeMillis();
				//Uploading the input file to the hdfs.
				if(args[0].startsWith("C")){ // in case of full path of input file in the local system.
					hdfs.copyFromLocalFile(new Path(args[0]),new Path("hdfs://127.0.0.1:9000/input/"));
				}else{ // in case of absolute path of input file in the local system.
					hdfs.copyFromLocalFile(new Path(new File("").getAbsolutePath().toString()+"\\"+Input),new Path("hdfs://127.0.0.1:9000/input/"));
				}
				//long tEnd = System.currentTimeMillis();
				//System.out.println((tEnd-tStart)/1000.0);
			}
		}else if (args[3].equals("angle") && Integer.parseInt(args[4])==2){
			if(Integer.parseInt(args[2])>1){
				//Only for 2 dimensional angle partitioning
				//long tStart = System.currentTimeMillis();
				ArrayList<Double> angles = new ArrayList<Double>(); // List of angles tan(fi)=dimension2/dimension1
				BufferedReader in; // Input reader of file
				String Line; // String buffer for reading file "line by line"
				String[] splits; //for parsing the line tuple apartment/data
				// Parsing from the number of tuples from the name of the input file
				String tuples=""; 
				for(int i=Input.length()-5;Input.charAt(i)>='0' && Input.charAt(i)<='9';i--){
					tuples=Input.charAt(i)+tuples;
				}
				System.out.println("Num of tuples : "+tuples);
				
				// If input file does not exist in the hdfs then
				// upload it line by line, while sampling the 10% of these lines 
				// to create a list of angles for the calculation of the bounds  
				if(!hdfs.exists(new Path("hdfs://127.0.0.1:9000/input/"+Input))){
					// Reader buffer of input file from local system
					if(args[0].startsWith("C")){ // in case of full path of input file in the local system.
						in = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])),4096);
					}else{ // in case of absolute path of input file in the local system.
						in = new BufferedReader(new InputStreamReader(new FileInputStream(new File("").getAbsolutePath().toString()+"\\"+Input)));
					}
					// Writer buffer for writing each line to the newly created file in the hdfs 
					BufferedWriter Writer = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path("hdfs://127.0.0.1:9000/input/"+Input))));
					Line=in.readLine(); //read line
					Random rnd = new Random(); // Declare random for sampling
					Double probability=0.9;
					Writer.write(Line); // Write the header of the .csv
					Writer.newLine(); // Write on a new line
					Line=in.readLine(); //read line
					
					// If input file is small enough, use all the data points to calculate the bounds 
					if(Integer.parseInt(tuples)==10 || Integer.parseInt(tuples)==100){
						probability=0.0;
					}
					
					//Writing-Sampling iteration
					while(Line!=null){
						Writer.write(Line); // Write line/tuple/data/apartment in hdfs input file
						Writer.newLine(); // Write on a new line
						if(rnd.nextDouble()>probability){ // Sampling
							splits = Line.split(",");
							//add a tan(fi)=dimension2/dimension1 angle in the list
							angles.add(Math.atan((Double.parseDouble(splits[2])/Double.parseDouble(splits[1]))));
						}	
						Line=in.readLine(); // read line from input file in local system
					}
					Writer.close(); // Close the write buffer Once the hole file is written in the hdfs.
				}else{
					// In this case the file exist in the hdfs
					// Read the fist 10% of the file to create the angles list for the calculation of the bounds
					
					// Reader buffer if the input file from the hdfs
					in= new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs://127.0.0.1:9000/input/"+Input))));
					Line=in.readLine(); // Getting rid of the header
					// Reading the first 10% data points, to create the list of angles
					Line=in.readLine();
					Double Percentage=0.1;
					
					// If input file is small enough, use all the data points to calculate the bounds 
					if(Integer.parseInt(tuples)==10 || Integer.parseInt(tuples)==100){
						Percentage=1.0;
					}
					for(int i=0;i<Math.round(Percentage*Integer.parseInt(tuples));i++){
						splits = Line.split(",");
						angles.add(Math.atan((Double.parseDouble(splits[2])/Double.parseDouble(splits[1]))));
						Line=in.readLine();
					}
				}
				in.close(); // Closing the reader buffer
				Collections.sort(angles); // Sorting the angles in ascending order
				//long tEnd = System.currentTimeMillis();
				//System.out.println("Time to execute : "+(tEnd-tStart)/1000.0);
				System.out.println("Size of list : "+angles.size());
				
		        //////////// Splitting the angles list to partitions, resulting to the boundaries ////////////
				int bound=Math.round(angles.size()/Integer.parseInt(args[2]))-1;
				int spl=Integer.parseInt(args[2]);
				bounds="";
				while(bound<angles.size()-Math.round(angles.size()/Integer.parseInt(args[2]))){
					if (bound!=Math.round(angles.size()/Integer.parseInt(args[2]))-1)
						bounds+=",";
					bounds+=angles.get(bound);
					spl--;
					bound+= Math.round((angles.size()-bound)/spl);
				}
				/////////////////////////////////////////////////////////////////////////////////////////////
			}else{
				if(!hdfs.exists(new Path("hdfs://127.0.0.1:9000/input/"+Input))){
					//long tStart = System.currentTimeMillis();
					//Uploading the input file to the hdfs.
					if(args[0].startsWith("C")){ // in case of full path of input file in the local system.
						hdfs.copyFromLocalFile(new Path(args[0]),new Path("hdfs://127.0.0.1:9000/input/"));
					}else{ // in case of absolute path of input file in the local system.
						hdfs.copyFromLocalFile(new Path(new File("").getAbsolutePath().toString()+"\\"+Input),new Path("hdfs://127.0.0.1:9000/input/"));
					}
					//long tEnd = System.currentTimeMillis();
					//System.out.println((tEnd-tStart)/1000.0);
				}
			}
			
		}else{
			// Something went wrong
			System.out.println("Something went wrong");
			System.exit(0);
		}
		
		System.out.println("Creating the Job . . .");
		
		// Creating the MapReduce Job
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/"+Input); // Input Path of input file in the hdfs for the job.
		Path outputPath = new Path("hdfs://127.0.0.1:9000/output/"); // Output Path of the output file to be created in the hdfs for the job.
		
		// Creating the job
		JobConf job = new JobConf(conf, Skyline.class);
		job.setJarByClass(Skyline.class);
		job.setJobName("SkylineQueriesJob");
		
		FileInputFormat.setInputPaths(job, inputPath); // Declaring input path of job
		FileOutputFormat.setOutputPath(job, outputPath); // Declaring output path of job
		
		job.set("splits",args[2]); // Declaring the number of partitions as a job argument
		job.set("partitioningTechnique",args[3]); // Declaring the partitioning technique as a job argument
		job.set("dimensions",args[4]); // Declaring number of dimensions as a job argument
		job.set("bounds",bounds); // Declaring the bounds of angle partitioning technique as a job argument
		job.setOutputKeyClass(IntWritable.class); // Declaring the keys of the job as an integer
		job.setOutputValueClass(Text.class); // Declaring the format of the input values of data points as Text
		job.setOutputFormat(TextOutputFormat.class); // Declaring the format of the output file of the job as Text
		job.setMapperClass(SkylineMapper.class); // Declaring the mapper class of the job
		job.setCombinerClass(SkylineCombiner.class); // Declaring the combiner class of the job
		job.setReducerClass(SkylineReducer.class); // Declaring the reducer class of the job
		
		// If the /output/ path exist the delete it to upload a new one.
		if (hdfs.exists(new Path("hdfs://127.0.0.1:9000/output/"))){
			hdfs.delete(new Path("hdfs://127.0.0.1:9000/output/"), true);
		}else{
			System.out.println("The result directory did not exist!");
		}
		
		//Running the MapReduce Job!
		System.out.println("About to start the Job!");
		long StartJob = System.currentTimeMillis();
		RunningJob runningJob = JobClient.runJob(job);
		long EndJob = System.currentTimeMillis();
		System.out.println("Job time : "+(EndJob-StartJob)/1000.0);
		System.out.println("job.isSuccessfull: " + runningJob.isComplete());
		
		// Renaming the output file as Skyline.csv
		hdfs.rename(new Path(outputPath.toString()+"/part-00000"), new Path(outputPath.toString()+"/"+args[1]));
		//Download the results in the local file system to the directory from with the jar was called
		hdfs.copyToLocalFile(new Path(outputPath.toString()+"/"+args[1]), new Path(new File("").getAbsolutePath().toString()));
	}
	
}
