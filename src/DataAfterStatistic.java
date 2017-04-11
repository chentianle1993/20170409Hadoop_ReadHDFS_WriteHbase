import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 从数据源文件中读取记录,处理后存储于TreeSet
 * 
 * @author guest
 *
 */
public class DataAfterStatistic {
	/**
	 * 所有记录，使用TreeMap<Object, Object[]>实现，
	 * 
	 * key表示sortKey，int型或string型
	 * value中的类型为Integer,Float两种，输出时需要强制转换
	 * 
	 * (Integer)Object[n] 表示相同key的记录的count，
	 * (Float)Object[0]表示第零个统计操作的结果，(Float)Object[1]表示第一个统计操作的结果
	 * 
	 */
	private static TreeMap<Object, Object[]> allRecords = new TreeMap<>();
	
	
	public static TreeMap<Object, Object[]> getAllRecords() {
		return allRecords;
	}
	
	/**
	 * 从数据源文件中读取数据集,并存储于TreeSet
	 */
	public static void setDataRecords() {
		//打开数据源文件
		Configuration conf=new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(CommandLineArgs.getSourceDataFilePath()),conf);
		} catch (IOException e) {
			// 
			e.printStackTrace();
		}
		Path path = new Path(CommandLineArgs.getSourceDataFilePath());
		FSDataInputStream in_stream = null;
		try {
			in_stream = fs.open(path);
		} catch (IOException e) {
			// 
			e.printStackTrace();
		}

		//遍历文件的每一行,注意Key值不允许相同！！！！！！！
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s = new String();
		try {
			while ((s=in.readLine())!=null) {
				String[] columns = s.split("\\|");
				try {
					Integer sortKey=Integer.valueOf(columns[CommandLineArgs.getGroupByLine()]);
					addRecordAndDeal(sortKey, columns);
				} catch (NumberFormatException e) {
					try{
						Float sortKey=Float.valueOf(columns[CommandLineArgs.getGroupByLine()]);
						addRecordAndDeal(sortKey, columns);
					}catch(NumberFormatException e2){
						String sortKey=columns[CommandLineArgs.getGroupByLine()];
						addRecordAndDeal(sortKey, columns);
					}
				}
			}
		} catch (IOException e) {
			// 
			e.printStackTrace();
		}
		try {
			in.close();
		} catch (IOException e) {
			// 
			e.printStackTrace();
		}
		try {
			fs.close();
		} catch (IOException e) {
			// 
			e.printStackTrace();
		}
	}
	/**
	 * 因为TreeMap中的Key不允许重复，因此，遇到key重复的记录时，需将记录添加到key 对应的value
	 * 
	 * 处理函数在此处处理
	 * @param sortKey
	 * @param s
	 */
	private static void addRecordAndDeal(Object sortKey, String[] recordColumns) {
		 Object[] resOfSearch = allRecords.get(sortKey);
		 if(resOfSearch==null){//无记录存储
			 resOfSearch= new Object[CommandLineArgs.getStatisticsOperations().length+1];
			 resOfSearch[CommandLineArgs.getStatisticsOperations().length]=Integer.valueOf(0);
			//resOfSearch[]={0,};
			 
			 allRecords.put(sortKey, resOfSearch);
			 updateGroup(resOfSearch,recordColumns);
		 }else{//原有记录存储
			 updateGroup(resOfSearch,recordColumns);
		 }
	}
	
	/**
	 * 将记录合并到TreeMap中相同sortKey的记录中
	 * @param resOfSearch	在TreeMap中与recordColumns有相同sortkey的结果
	 * @param recordColumns	当前的一条记录
	 */
	private static void updateGroup(Object[] resOfSearch, String[] recordColumns) {		
		for(int i=0;i<CommandLineArgs.getStatisticsOperations().length;i++){
			resOfSearch[i]=setOneResColumnInTreeMapValue((Integer)resOfSearch[CommandLineArgs.getStatisticsOperations().length],resOfSearch[i],CommandLineArgs.getStatisticsOperations()[i],recordColumns[CommandLineArgs.getStatisticsColumnsNum(i)]);
		}
		//将同一个sortkey的记录的计数加一
		resOfSearch[CommandLineArgs.getStatisticsOperations().length]=Integer.valueOf((Integer)resOfSearch[CommandLineArgs.getStatisticsOperations().length]+1);
	}
	/**
	 * 对当前处理的一条记录更新TreeMap中的一个统计值int[i]
	 * @param recordsHasDealedCount 已经处理过的相同sortKey的记录的数量
	 * @param lastResInTreeMap	在TreeMap中已存储的以前的结果
	 * @param opreationString	需要做的操作的字符串
	 * @param columnToOperate	操作对象
	 * @return	操作结果
	 */
	private static Object setOneResColumnInTreeMapValue(int recordsHasDealedCount, Object lastRes, String opreationString, String columnToOperate) throws UnsupportedOperationException{
		if(opreationString.startsWith("count")){
			//是count操作
			if(lastRes==null){
				lastRes=Integer.valueOf(0);
			}
			return Integer.valueOf((Integer)lastRes+1);
		}
		if(opreationString.startsWith("avg")){
			//是avg操作
			
			//先求已经处理的记录的总和,之前的avg以float存储
			Float sumOfPassedRecords=Float.valueOf((float) 0.0);
			if(lastRes!=null){
				sumOfPassedRecords=(Float)lastRes*recordsHasDealedCount;
			}
			sumOfPassedRecords+=Float.valueOf(columnToOperate);
			
			return sumOfPassedRecords/(recordsHasDealedCount+1);
		}
		if(opreationString.startsWith("max")){
			//是max操作
			if(lastRes==null||Float.valueOf(columnToOperate)>(Float)lastRes){
				lastRes=Float.valueOf(columnToOperate);
			}
			return lastRes;
		}
		if(opreationString.startsWith("sum")){
			//是sum操作
			if(lastRes==null){
				lastRes=Integer.valueOf(0);
			}
			Integer thisValue=Integer.valueOf(columnToOperate);

			return (Integer)lastRes+(Integer)thisValue;
		}
		
		
		
		//执行到此处还未返回，则说明不是count,avg,max三种操作
		throw new UnsupportedOperationException();
	}

	public static Object[] getFirstValue_poll() {
		// TODO Auto-generated method stub
		return allRecords.pollFirstEntry().getValue();
	}


}
