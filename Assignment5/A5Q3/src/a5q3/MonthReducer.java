/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class MonthReducer extends Reducer<IntWritable,Text,Text,NullWritable>{
    protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
        for(Text t:values){
            System.out.println(t);
            context.write(t, NullWritable.get());
        }
    }
}
