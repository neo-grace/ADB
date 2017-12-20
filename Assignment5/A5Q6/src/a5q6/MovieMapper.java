/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q6;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class MovieMapper extends Mapper<Object,Text,Text,Text>{
    private Text outkey = new Text();
    private Text outvalue = new Text();
    
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException
    {
        String str[] = value.toString().split(",");
        if(str[0].equals("movieId"))
        {
            return;
        }
        else
        {
        outkey.set(str[0]);
        outvalue.set("M"+str[1]);
        context.write(outkey,outvalue);
        }
    }
}
