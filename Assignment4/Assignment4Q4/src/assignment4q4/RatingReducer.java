/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q4;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class RatingReducer extends Reducer<IntWritable,SortedMapWritable,IntWritable,MedianCustomWritable>{
    private MedianCustomWritable result = new MedianCustomWritable();
    private TreeMap<Float,Long> ratingsCount = new TreeMap<Float,Long>();
    
   public void reduce(IntWritable key,Iterable<SortedMapWritable> values,Context context) throws IOException, InterruptedException
   {
       float sum=0;
       long totalRating = 0;
       ratingsCount.clear();
       result.setMedian(0);
       result.setStandardDeviation(0);
       
       for(SortedMapWritable v:values)
       {
           for(Entry<WritableComparable,Writable> entry:v.entrySet()){
               float rate = ((FloatWritable)entry.getKey()).get();
               long count = ((LongWritable)entry.getValue()).get();
           
           
                totalRating += count;
                sum += rate * count;
                
                Long storedCount = ratingsCount.get(rate);
                if(storedCount == null){
                    ratingsCount.put(rate, count);
                }
                else{
                    ratingsCount.put(rate, storedCount+count);
                }
           }
       }
       long medianIndex = totalRating/2L;
       long previousRate = 0;
       long ratings = 0;
       float prevKey = 0;
       for(Entry<Float,Long> entry:ratingsCount.entrySet()){
           ratings = previousRate + entry.getValue();
           
           if(previousRate <= medianIndex && medianIndex < ratings){
               if(totalRating % 2 == 0 && previousRate < medianIndex){
                   result.setMedian((float)(entry.getKey() + prevKey)/2.0f);
               }else
               {
                   result.setMedian(entry.getKey());
               }
               break;
           }
           previousRate = ratings;
           prevKey = entry.getKey();
       }
             
       float mean = sum/totalRating;
       float sumOfSquares = 0.0f;
       for(Entry<Float,Long> entry:ratingsCount.entrySet()){
           sumOfSquares += (entry.getKey() - mean)*(entry.getKey()-mean)*entry.getValue();
       }
       result.setStandardDeviation((float)Math.sqrt(sumOfSquares/(totalRating-1)));
       context.write(key, result);
       
   }
}
