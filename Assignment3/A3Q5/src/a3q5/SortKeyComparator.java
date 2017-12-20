/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a3q5;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author neola
 */
public class SortKeyComparator extends WritableComparator{
    
    protected SortKeyComparator()
    {
        super(FloatWritable.class,true);
    }
    
    @Override
	public int compare(WritableComparable a, WritableComparable b) {
		FloatWritable key1 = (FloatWritable) a;
		FloatWritable key2 = (FloatWritable) b;

		// Implemet sorting in descending order
		int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
		return result;
	}
    
    
    
    
    
    
    
    
    
}

