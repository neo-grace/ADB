/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q2;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class StockMapper extends Mapper<Object,Text,IntWritable,Stock>{
    
    IntWritable year = new IntWritable();
    Stock outStock = new Stock();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
        
        String str[] = value.toString().split(",");
        if(str[0].equals("exchange")) {
            return;
        } 
        
        
        Float stockVal = Float.parseFloat(str[8]);
        year.set(Integer.parseInt(str[2].substring(0, 4)));   
        
            
            outStock.setStockValAvg(stockVal);
            outStock.setCount(1);
            context.write(year,outStock);
        }
        catch(Exception e)
         {
             System.out.println("Not a numeric value..");
         }
    }
    
}
