/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cancerresearch;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class CancerSiteReducer extends Reducer<IntWritable,LongWritable,IntWritable,LongWritable>{

    private LongWritable result = new LongWritable();
    
    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for(LongWritable val:values)
        {
            sum += val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
    
}
