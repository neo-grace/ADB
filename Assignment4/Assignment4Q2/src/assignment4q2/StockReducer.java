/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class StockReducer extends Reducer<IntWritable,Stock,IntWritable,Stock>{
    
    Stock result = new Stock();

    @Override
    protected void reduce(IntWritable key, Iterable<Stock> values, Context context) throws IOException, InterruptedException {
            
        int sum = 0;
        int count = 0;
             
        for(Stock val:values)
        {
            
            sum += val.getCount() * val.getStockValAvg();
            count += val.getCount();
        }

        result.setStockValAvg((float)sum/count);
        result.setCount(count);       

        context.write(key, result);

    }
    
    
    
}
