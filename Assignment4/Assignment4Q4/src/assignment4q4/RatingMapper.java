/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q4;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class RatingMapper extends Mapper<Object,Text,IntWritable,SortedMapWritable>{
           
    private static final LongWritable one = new LongWritable(1);
    private FloatWritable ratingMovie = new FloatWritable();
    
    public void map(Object key, Text value, Context context){
         try{
             
                String row[] = value.toString().split("::");
                Integer movie = Integer.parseInt(row[1]);
                Float rating = Float.parseFloat(row[2]);
                ratingMovie.set(rating);
                
                SortedMapWritable outRating = new SortedMapWritable();
                outRating.put(ratingMovie, one);
                
                context.write(new IntWritable(movie), outRating);
                
            }
            catch(Exception e){
                
            }
        }       
    
}
