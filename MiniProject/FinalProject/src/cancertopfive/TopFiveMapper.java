/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cancertopfive;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class TopFiveMapper extends Mapper<IntWritable,LongWritable,NullWritable,IntWritable> {
// Stores a map of user reputation to the record
    private TreeMap<LongWritable,IntWritable> repToRecordMap = new TreeMap<LongWritable,IntWritable>();
    private IntWritable outkey = new IntWritable();
    private LongWritable outcount = new LongWritable();
    
    public void map(IntWritable key, LongWritable value, Context context) throws IOException, InterruptedException
 {
       String str[] = value.toString().split(",");
       outkey.set(Integer.parseInt(str[0]));
       outcount.set(Long.parseLong(str[1]));
       System.out.println(str[0]);
       System.out.println(str[1]);
       repToRecordMap.put(outcount,outkey);
       if(repToRecordMap.size() > 5) {
           repToRecordMap.remove(repToRecordMap.firstKey());
        }
 }
protected void cleanup(Context context) throws IOException,InterruptedException
 {
    for(IntWritable t : repToRecordMap.values()) {
        context.write(NullWritable.get(), t);
    }
  }
}

