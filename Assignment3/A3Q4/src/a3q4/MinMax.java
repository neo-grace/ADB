/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a3q4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 * @author neola
 */
public class MinMax implements Writable{
    
    String minStockVol;
    String maxStockVol;
    String maxStockPriceAdj;
    String minStock;
    String maxStock;

    public String getMinStockVol() {
        return minStockVol;
    }

    public void setMinStockVol(String minStockVol) {
        this.minStockVol = minStockVol;
    }

    public String getMaxStockVol() {
        return maxStockVol;
    }

    public void setMaxStockVol(String maxStockVol) {
        this.maxStockVol = maxStockVol;
    }

    public String getMaxStockPriceAdj() {
        return maxStockPriceAdj;
    }

    public void setMaxStockPriceAdj(String maxStockPriceAdj) {
        this.maxStockPriceAdj = maxStockPriceAdj;
    }

    public String getMinStock() {
        return minStock;
    }

    public void setMinStock(String minStock) {
        this.minStock = minStock;
    }

    public String getMaxStock() {
        return maxStock;
    }

    public void setMaxStock(String maxStock) {
        this.maxStock = maxStock;
    }
    
    
   
    @Override
    public void write(DataOutput d) throws IOException {

         WritableUtils.writeString(d, minStockVol);
        WritableUtils.writeString(d, maxStockVol);
         WritableUtils.writeString(d, maxStockPriceAdj);
        WritableUtils.writeString(d, minStock);
         WritableUtils.writeString(d, maxStock);
        
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        minStockVol = WritableUtils.readString(di);
        maxStockVol = WritableUtils.readString(di);
        maxStockPriceAdj = WritableUtils.readString(di);
        minStock = WritableUtils.readString(di);
        maxStock = WritableUtils.readString(di);
    }
    
    public String toString()
    {
        return (new StringBuilder().append(minStockVol).append("\t").append(maxStockVol).toString());
    }
    
}
