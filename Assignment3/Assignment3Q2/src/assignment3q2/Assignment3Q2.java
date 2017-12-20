/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3q2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author neola
 */
public class Assignment3Q2 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        System.out.println("Start Time:" +dateFormat.format(date)); 
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
        
        Path inputDir = new Path(args[1]);
        Path hdfsFile = new Path(args[2]);
        
        try{
            FileStatus[] inputFiles = local.listStatus(inputDir);
            FSDataOutputStream out = hdfs.create(hdfsFile);
            for(int i=0; i<inputFiles.length;i++){
                //System.out.println(inputFiles[i].getPath().getName());
                FSDataInputStream in = local.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer,0,bytesRead);
                }
                in.close();
            }
                out.close();
                Job job = Job.getInstance(conf, "nyseAvg");
        
        job.setJarByClass(Assignment3Q2.class);
        job.setMapperClass(CsvReadMapper.class);
        job.setCombinerClass(CsvReadReducer.class);
        job.setReducerClass(CsvReadReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        if(job.waitForCompletion(true) == true)
        {
            date = new Date();
            System.out.println("End Time :" + dateFormat.format(date));
            System.exit(0);
        }
        else
        {
            System.exit(1);
        }
        
            }
        
            catch(IOException e){
                    e.printStackTrace();
                    }
        }
        
        
        
        
        
        
        
        
        
        
       
        
        
    }
    

