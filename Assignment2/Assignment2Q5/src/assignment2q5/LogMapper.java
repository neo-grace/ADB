/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment2q5;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neolap
 */
public class LogMapper extends Mapper<Object,Text,Text,IntWritable>{
     private final static IntWritable count = new IntWritable(1);
    private Text ipadd = new Text();
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException
    {
        String str[] = value.toString().split(" ");
        ipadd.set(str[0]);
        context.write(ipadd,count);
        
    } 
}
