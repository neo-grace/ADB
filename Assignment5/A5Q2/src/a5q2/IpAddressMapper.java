/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q2;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class IpAddressMapper extends Mapper<Object, Text, Text, NullWritable>{
    private Text outIpAddress = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String input[] = value.toString().split(" ");
        outIpAddress.set(input[0]);
        context.write(outIpAddress, NullWritable.get());
    }
}
