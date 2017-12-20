/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q3;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import jdk.nashorn.internal.objects.NativeString;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class MonthMapper extends Mapper<Object,Text,IntWritable,Text>{

    private IntWritable outkey = new IntWritable();
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    
    protected void map(Object key,Text value,Context context) throws IOException, InterruptedException
    {
        try {
            String str[] = value.toString().split(" ");
            String date = str[3].substring(1, str[3].length());
            Calendar cal = Calendar.getInstance();
            cal.setTime(sdf.parse(date));
            outkey.set(cal.get(Calendar.MONTH));
            context.write(outkey, value);
        } catch (ParseException ex) {
            Logger.getLogger(MonthMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
