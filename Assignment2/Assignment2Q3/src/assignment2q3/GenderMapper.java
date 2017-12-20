/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment2q3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neolap
 */
public class GenderMapper extends Mapper<Object,Text,Text,IntWritable>{
    private static Text gender = new Text();
    private static IntWritable count = new IntWritable(1);
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        String str[] = value.toString().split("::");
        gender.set(str[1]);
        context.write(gender, count);
    }
}