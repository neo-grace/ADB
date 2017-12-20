/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment4q2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author neola
 */
public class Stock implements Writable{
    
    Integer count;
    Float stockValAvg;

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    

    public Float getStockValAvg() {
        return stockValAvg;
    }

    public void setStockValAvg(Float stockValAvg) {
        this.stockValAvg = stockValAvg;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(count);
        d.writeFloat(stockValAvg);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        count = di.readInt();
        stockValAvg = di.readFloat();
    }

    @Override
    public String toString() {
        return stockValAvg + "\t"; //To change body of generated methods, choose Tools | Templates.
    }

    

}
