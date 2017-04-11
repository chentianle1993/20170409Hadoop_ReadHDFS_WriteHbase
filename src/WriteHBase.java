import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * 将处理过的数据集写出到HBase
 * 
 * @author guest
 *
 */
public class WriteHBase {
	/**
	 * 将处理过的数据集写出到HBase
	 */
	public static void setHbaseTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		
		Logger.getRootLogger().setLevel(Level.WARN);
		
		//create table descriptor
		String tableName="Result";
		HTableDescriptor hBaseTable=new HTableDescriptor(TableName.valueOf(tableName));
		
		//create column family
		HColumnDescriptor hBaseFamily=new HColumnDescriptor("res");
		hBaseTable.addFamily(hBaseFamily);
		//configure HBase
		Configuration hbaseConfig=HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin;
		hBaseAdmin = new HBaseAdmin(hbaseConfig);
		
		//删除原始的Result表
		if(hBaseAdmin.tableExists(tableName)){
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}
		hBaseAdmin.createTable(hBaseTable);
		hBaseAdmin.close();
		HTable table=new HTable(hbaseConfig, tableName);
				
		//遍历TreeMap
		Put put=null;
		//用于保留两位小数
		DecimalFormat decimalFormat=new DecimalFormat(".00");
		Object statisticsRes=null;
		String statisticsResString="";
		Map.Entry<Object, Object[]> thisEntry=null;
		
		for(thisEntry=DataAfterStatistic.getAllRecords().pollFirstEntry();
			thisEntry!=null;
			thisEntry=DataAfterStatistic.getAllRecords().pollFirstEntry())
		{//遍历每一列
			//对于同一行来说，prim key是相同的
			put=new Put(String.valueOf(thisEntry.getKey()).getBytes());
			
			for(int columnNum=0;columnNum<CommandLineArgs.getStatisticsOperations().length;columnNum++){
				//统计结果可能是float，也可能是int型
				statisticsRes=thisEntry.getValue()[columnNum];
				if(statisticsRes.getClass()==Float.class){
					statisticsResString=decimalFormat.format(statisticsRes).toString();
				}else{
					statisticsResString=String.valueOf(statisticsRes);
				}
				//例如put.add("res","avg(R3)","32.15")
				
				put.add("res".getBytes(), CommandLineArgs.getStatisticsOperations()[columnNum].getBytes(), statisticsResString.getBytes());
			}
			table.put(put);
		}
		table.close();
		//System.out.println("put Successfully");
	}
}
