/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author neola
 */
public class A5Q5 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO code application logic here
         Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "InnerJoin");
        job.setJarByClass(A5Q5.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Users.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, BookRatingMapper.class);
        job.getConfiguration().set("join.type", "inner");
        job.setReducerClass(UserBookRatingReducer.class);
        //job.setOutputValueClass(Text.class);
        //job.setOutputKeyClass(Text.class);
        
        //job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        boolean flag = job.waitForCompletion(true);
        
        Configuration configuration2 = new Configuration();
        Job job2 = Job.getInstance(configuration2, "Chaining");

        if(flag){
            job2.setJarByClass(A5Q5.class);
            MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, IntermediateMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, Books.class);
            job2.getConfiguration().set("join.type", "inner");
            job2.setReducerClass(BookBookRatingReducer.class);
            //job2.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[5]));
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
    }
    
}
