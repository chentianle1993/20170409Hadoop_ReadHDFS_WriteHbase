import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

/**
 * 主类
 * 
 * 在HDFS中读取文件，按照输入参数中指定的行进行sort based group-by,然后写入HBase
 * 
 * @author 201628016029031
 *
 */
public class Hw1Grp3 {
	/**
	 * 主函数
	 * @param args
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//处理命令行参数
		CommandLineArgs.setCommandArgs(args);
		//从数据源文件中读取数据集,并存储于TreeSet
		DataAfterStatistic.setDataRecords();
		//将处理过的数据集写出到HBase
		WriteHBase.setHbaseTable();
		
	}
}

