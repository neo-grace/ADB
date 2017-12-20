/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a3q4;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author neola
 */
public class MinMaxReducer extends Reducer<Text,MinMax,Text,Text>{
    
   
    
  
    public void reduce(Text key, Iterable<MinMax> values,Context context) throws IOException, InterruptedException{
        
        String minStockVol = "";
        String maxStockVol = "";
        Float maxStockPrice = 0f;
        Integer minStock = 0;
        Integer maxStock = 0;
       for(MinMax m:values){
        if(minStock > (Integer.parseInt(m.getMinStock())))
            minStockVol = m.getMinStockVol();
        if(maxStock < (Integer.parseInt(m.getMaxStock())))
            maxStockVol = m.getMaxStockVol();
        if(maxStockPrice < (Float.parseFloat(m.getMaxStockPriceAdj())))
            maxStockPrice = Float.parseFloat(m.getMaxStockPriceAdj());
        }
        String temp = minStockVol + "\t" + maxStockVol + "\t" + String.valueOf(maxStockPrice);
        Text text = new Text(temp);
        context.write(key, text);
    }    
}
