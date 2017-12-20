/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a3q5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author neola
 */
public class A3Q5 {

   
    
     public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final static FloatWritable rating = new FloatWritable();
        private Text movie = new Text();

        public void map(LongWritable key, Text value, Context context) {

            String row[] =  value.toString().split("::");
            String movieId = row[1];
            String rate = row[2].trim();

            try{
                FloatWritable rating =  new FloatWritable(Float.parseFloat(rate));
                context.write(new Text(movieId), rating);
            } catch (Exception e) {

            }

        }
    }
    
      public static class Reduce1 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable totalRating = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            int counter = 0;
            int avg = 0;
            for(FloatWritable val : values) {
                counter++;
                sum += val.get();
            }
            avg = sum/counter;
            totalRating.set(avg);
            context.write(key, totalRating);
        }
      }
        
        public static class Map2 extends Mapper<LongWritable, Text, FloatWritable, Text> {

        public void map(LongWritable key, Text value, Mapper.Context context) {

            String row[] =  value.toString().split("\\t");
            Text movieId = new Text(row[0]);
            String rate = row[1].trim();

            try{
                FloatWritable count =  new FloatWritable(Float.parseFloat(rate));
                context.write(count, movieId);
            } catch (Exception e) {

            }

        }
    }
        
        public static class Reduce2 extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        int count = 0;
        
        @Override
        protected void setup(Context context)
        {
            count = 0;
        }
        
        public void reduce(FloatWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
             if(count < 25)
                {
                    for(Text val : value) {
                
               
                    context.write(val, key);
                    count++;
                    if(count > 25)
                        break;
                }
                
            }
                
        }
    }
        
    
    
    
 public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration configuration = new Configuration();
        Job job1 = Job.getInstance(configuration, "top25");
        job1.setJarByClass(A3Q5.class);
        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);

        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        boolean complete = job1.waitForCompletion(true);

        Configuration configuration2 = new Configuration();
        Job job2 = Job.getInstance(configuration2, "top25");

        if(complete){
            job2.setJarByClass(A3Q5.class);
            job2.setMapperClass(Map2.class);
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setSortComparatorClass(SortKeyComparator.class);
            job2.setReducerClass(Reduce2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
    
}
