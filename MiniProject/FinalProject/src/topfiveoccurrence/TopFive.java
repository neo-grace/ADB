/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package topfiveoccurrence;

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
public class TopFive {
    
    public static class Map1 extends Mapper<IntWritable, LongWritable, LongWritable, IntWritable> {

        public void map(IntWritable key, LongWritable value, Context context) {

            String row[] =  value.toString().split(",");
            String id = row[0];
            String count = row[1].trim();

            try{
                LongWritable readCount =  new LongWritable(Long.parseLong(count));
                context.write(readCount,new IntWritable(Integer.parseInt(id)));
            } catch (Exception e) {

            }

        }
    }
    
    public static class Reduce1 extends Reducer<LongWritable,IntWritable, IntWritable, LongWritable> {
    int count = 0;
        
        @Override
        protected void setup(Context context)
        {
            count = 0;
        }
        
        public void reduce(LongWritable key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
             if(count < 5)
                {
                    for(IntWritable val : value) {         
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
        Job job = Job.getInstance(configuration, "top5");
        job.setJarByClass(TopFive.class);
        job.setMapperClass(Map1.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(SortKeyComparator.class);
        job.setReducerClass(Reduce1.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
