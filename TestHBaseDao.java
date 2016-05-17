package com.cares.nrise.hbase;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.util.Bytes;  


public class TestHBaseDao implements Serializable {
	private static String split="@MCPCAR@";
	static Configuration cfg = new Configuration();
	static {
		cfg.set("hbase.zookeeper.quorum", "192.168.206.26");   
		cfg.set("hbase.master", "192.168.206.26");   
	}
	
	private static void testDate() throws IOException{
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(cfg, "testtable");
	//	HBaseAdmin admin = new HBaseAdmin(config); 
		Get get = new Get(Bytes.toBytes("row-1"));
		get.setMaxVersions(10);
		get.addColumn(Bytes.toBytes("clomf1"), Bytes.toBytes("aa"));  
	    Result result = table.get(get);
	    List<KeyValue> list = result.list();   
	     for(final KeyValue kv:list){  
	         System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",   
	                 Bytes.toString(kv.getRow()),   
	                 Bytes.toString(kv.getFamily()),   
	                 Bytes.toString(kv.getQualifier()),   
	                 Bytes.toString(kv.getValue()),  
	                 kv.getTimestamp()));       
	     }
		System.gc();
	}
	private static void putDate(){
		List<Put> list = new ArrayList<Put>();
		Configuration config = HBaseConfiguration.create();
		try {
			HTable table = new HTable(cfg, "testtable");
//			table.setAutoFlush(false);
			Put put = new Put(Bytes.toBytes("row-1"));
			for(int i=5;i<10;i++){
				  put.add(("clomf1").getBytes(),Bytes.toBytes("aa"),Bytes.toBytes(String.valueOf(i+15)));
				  table.put(put);
			}
		  table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		}
      
	}
	
	private static void modifyTable() throws MasterNotRunningException,ZooKeeperConnectionException,IOException{
	    Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists("testtable")) {
            try {
                admin.disableTable("testtable");
                HTableDescriptor htd =admin.getTableDescriptor(Bytes.toBytes("testtable"));
                HColumnDescriptor infocf = htd.getFamily(Bytes.toBytes("clomf1"));
                infocf.setMaxVersions(8);
                admin.modifyTable(Bytes.toBytes("testtable"),htd);
                admin.enableTable("testtable");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        admin.close();
	}
	
	private static void opTable() throws Exception{
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("testtable");
		HColumnDescriptor hcd = new  HColumnDescriptor("clomf1");
		hcd.setMaxVersions(5);
		htd.addFamily(hcd);
		admin.createTable(htd);
	    admin.close();
		
	}
    public static void main(String[] args) throws Exception{
//    	try {
//    		opTable();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//    	putDate();
    	try {
			testDate();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//    	modifyTable();
	}
	private static String getKey(byte[] key){
		String[] s=(new String(key)).split("/");
		return s[1]+"/"+s[2]+"/"+s[3]+"/"+s[4];
	}
	public static String doubleFormat(Double d) {
		java.text.DecimalFormat df = new java.text.DecimalFormat("#.00");
		return df.format(d);
	}

	public static Integer getStrInteger(String s) {
		try {
			return Integer.parseInt(s);
		} catch (Exception e) {
			return 0;
		}
	}

	public static double getStrDouble(String s) {
		try {
			return Double.parseDouble(s);
		} catch (Exception e) {
			return 0D;
		}
	}

	public static String getDateLong() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}
}
