/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author neola
 */
public class HourMapper extends Mapper<Object, Text, Text, NullWritable>{
        private MultipleOutputs<Text, NullWritable> mos;

        
        protected void setup(Context context){
            mos = new MultipleOutputs(context);
        }
        
        protected void map(Object key,Text value,Context context) throws IOException, InterruptedException
        {
            try 
            {
                String str[] = value.toString().split(" ");
                if(str.length > 4)
                {
                String logAccess = str[3].trim(); 
                logAccess = logAccess.substring(13, 15);
                logAccess = logAccess + " hrbin";
                mos.write("bins", value, NullWritable.get(), logAccess);
                }
            } catch (Exception ex) {
                Logger.getLogger(HourMapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        protected void cleanup(Context context) throws IOException, InterruptedException{
            mos.close();
        }
}
