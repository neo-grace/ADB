/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author neola
 */
public class Assignment4Q4 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "medianstd");
        job.setJarByClass(Assignment4Q4.class);
        job.setMapperClass(RatingMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SortedMapWritable.class);
        job.setCombinerClass(RatingCombiner.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SortedMapWritable.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedianCustomWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      
    }
    
}
