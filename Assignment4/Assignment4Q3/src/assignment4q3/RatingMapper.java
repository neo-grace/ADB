/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class RatingMapper extends Mapper<Object,Text,IntWritable,DoubleWritable>{
    
    public void map(Object key, Text value, Context context){
            try{
                String row[] = value.toString().split("::");
                Integer movie = Integer.parseInt(row[1]);
                Double rating = Double.parseDouble(row[2]);                
                context.write(new IntWritable(movie), new DoubleWritable(rating));
                
            }catch(Exception e){
                
            }
        }       
    
}
