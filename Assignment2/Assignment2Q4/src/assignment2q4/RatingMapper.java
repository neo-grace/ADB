/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment2q4;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neolap
 */
public class RatingMapper extends Mapper<Object,Text,Text,IntWritable>{
    private static IntWritable setcount = new IntWritable(1);
    private static Text userid = new Text();
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException
    {
        String str[] = value.toString().split("::");
        userid.set(str[0]);
        context.write(userid, setcount);
    }
    
}
