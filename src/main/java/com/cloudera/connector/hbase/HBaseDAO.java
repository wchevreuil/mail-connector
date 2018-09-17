package com.cloudera.connector.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.model.CaseMail;

public class HBaseDAO {
	
	private Connection connection;
	
	private Table table;
	
	private List<Put> putList = new ArrayList<Put>();
	
	public HBaseDAO() throws IOException{
		
		this.connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
		
		this.table = connection.getTable(TableName.valueOf("cases"));
		
	}

	public void insertCaseMail(CaseMail mail){
		Put p = new Put(
				Bytes.toBytes(mail.getId().hashCode() + "-" + mail.getId()));
		p.addColumn(Bytes.toBytes("msgs"), Bytes.toBytes(mail.getTimestamp()), Bytes.toBytes(mail.getDescription()));
		if(mail.getComponent() != null)
			p.addColumn(Bytes.toBytes("sum"), Bytes.toBytes("comp"), Bytes.toBytes(mail.getComponent()));
		if(mail.getSubComponent()!=null)
			p.addColumn(Bytes.toBytes("sum"), Bytes.toBytes("subc"), Bytes.toBytes(mail.getSubComponent()));
		if(mail.getAccount()!=null)
			p.addColumn(Bytes.toBytes("sum"), Bytes.toBytes("acc"), Bytes.toBytes(mail.getAccount()));
		if(mail.getSubject()!=null)
			p.addColumn(Bytes.toBytes("sum"), Bytes.toBytes("subj"), Bytes.toBytes(mail.getSubject()));
		
		putList.add(p);
	}
	
	public void flush() throws IOException{
		
		this.table.put(putList);
	}
	
	public void close() throws IOException{
		
		this.connection.close();
	}
}
