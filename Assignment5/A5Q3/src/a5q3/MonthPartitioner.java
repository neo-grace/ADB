/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package a5q3;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author neola
 */
public class MonthPartitioner extends Partitioner<IntWritable,Text> implements Configurable
        {

    private static final String MIN_DATE_MONTH = "min.date.month";
    private Configuration conf = null;
    private int minDateMonth = 0;
    
    @Override
    public int getPartition(IntWritable key, Text value, int i) {
        return key.get() - minDateMonth;
    }

    @Override
    public void setConf(Configuration c) {
        this.conf = c; 
        minDateMonth = conf.getInt(MIN_DATE_MONTH, 0);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
    
    public static void setMinDate(Job job,int minDateMonth){
        job.getConfiguration().setInt(MIN_DATE_MONTH, minDateMonth);
    }
}
