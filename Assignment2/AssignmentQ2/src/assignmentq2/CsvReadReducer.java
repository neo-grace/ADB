/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignmentq2;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neolap
 */
public class CsvReadReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
    FloatWritable res = new FloatWritable();
    
    public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException
    {
        float avg = 0;
        int counter = 0;
        float sum = 0;
        for(FloatWritable value:values)
        {
            counter++;
            sum+=value.get();
        }
        avg = sum/counter;
        res.set(avg);
        context.write(key, res);
    }
    
}
