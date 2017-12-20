/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author neola
 */
public class Assignment4Q3 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "medStdDev");
        job.setJarByClass(Assignment4Q3.class);
        job.setMapperClass(RatingMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianSD.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    
}
