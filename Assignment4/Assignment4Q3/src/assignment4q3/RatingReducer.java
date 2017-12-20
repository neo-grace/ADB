/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class RatingReducer extends Reducer<IntWritable,DoubleWritable,IntWritable,MedianSD>{
    
    private MedianSD result = new MedianSD();
    private ArrayList<Double> list = new ArrayList<Double>();
    
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        
        double sum = 0;
        double count = 0;
        list.clear();
            result.setStandardDeviation(0);
            
            for(DoubleWritable val : values){
                list.add(val.get());
                sum += val.get();
                count++;            
            }
        
         Collections.sort(list);
            
            if(count % 2 == 0){
                result.setMedian((float)(list.get((int)count/2) + list.get((int)count/2 - 1))/2.0f);
               
            }else{
                result.setMedian(list.get((int) count/2));
            }
            
            double mean = sum/count;
            double sumOfSquares = 0;
            
            for(double val : list){
                sumOfSquares += (val - mean)*(val - mean);
            }
            result.setStandardDeviation((double) Math.sqrt(sumOfSquares / (count - 1)));
            context.write(key, result);
    }
    
}
