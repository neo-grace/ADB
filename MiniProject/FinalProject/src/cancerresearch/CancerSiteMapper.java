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
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class CancerSiteMapper extends Mapper<Object,Text,IntWritable,LongWritable>{
    private IntWritable outkey = new IntWritable();
    private LongWritable outvalue = new LongWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str[] = value.toString().split(",");
        if(str[0].equalsIgnoreCase("ParentId"))
            return;
        else
        {
            outkey.set(Integer.parseInt(str[0]));
            outvalue.set(Long.parseLong(str[5]));
            context.write(outkey, outvalue);
        }
    }   
}
