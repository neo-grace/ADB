/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cancertopfive;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class TopFiveReducer extends Reducer<NullWritable, IntWritable, NullWritable, Text> {

    private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
    public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws
    IOException, InterruptedException
    {
        for(IntWritable value: values) {
            String str[] = value.toString().split(",");
            repToRecordMap.put(Integer.parseInt(str[0]), new Text(str[1]));
            if(repToRecordMap.size() > 5) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }
        for(Text t : repToRecordMap.descendingMap().values()) {
            context.write(NullWritable.get(), t);
        }
    }
        
}
