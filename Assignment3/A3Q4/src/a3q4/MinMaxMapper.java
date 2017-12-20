/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a3q4;

import java.io.IOException;
import static java.lang.System.out;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author neola
 */
public class MinMaxMapper extends Mapper<Object, Text, Text, MinMax>{
    private Date minStockVol;
    private Date maxStockVol;
    private Float maxStockPriceAdj;
    private Integer minVol;
    private Integer maxVol;
    private Text stockSymbol = new Text();
    DateFormat formatter = new SimpleDateFormat("yyyy-mm-dd");
    
    
    
    
    
    public void map(Object key,Text value, Context context)throws IOException, InterruptedException
    {
       
        String str[] = value.toString().split(",");
        try{
            minStockVol = formatter.parse(str[2]);
            maxStockVol = formatter.parse(str[2]);
            maxStockPriceAdj = Float.parseFloat(str[8]);
            minVol = Integer.parseInt(str[7]);
            maxVol = Integer.parseInt(str[7]);
            stockSymbol.set(str[1]);
            MinMax out1 = new MinMax();
            out1.setMinStockVol(minStockVol.toString());
            out1.setMaxStockVol(maxStockVol.toString());
            out1.setMaxStockPriceAdj(maxStockPriceAdj.toString());
            out1.setMaxStock(maxVol.toString());
            out1.setMinStock(minVol.toString());
            
            context.write(stockSymbol, out1);
        }
        catch(Exception e)
        {
        }
    }
    
}
