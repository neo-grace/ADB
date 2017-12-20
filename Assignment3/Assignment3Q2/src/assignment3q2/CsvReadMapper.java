/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3q2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neolap
 */
public class CsvReadMapper extends Mapper<Object,Text,Text,FloatWritable>{
    private static FloatWritable val;
    private static Text text;
    
    
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException
    {
        
        String str[] = value.toString().split(",");
        try
        {
            if(str[0].equals("exchange"))
                return;
            else
            {
                val = new FloatWritable(Float.parseFloat(str[4]));
                text = new Text(str[1]);        
                context.write(text, val);
            }
        }
        
         catch(Exception e)
         {
             System.out.println("Not a numeric value.." + e);
         }
    }
    
}
