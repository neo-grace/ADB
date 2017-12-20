/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q2;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class IpAddressReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
    
    public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException
    {
        context.write(key, NullWritable.get());
    }
}
