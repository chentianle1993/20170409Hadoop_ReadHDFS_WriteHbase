
/**
 * 处理命令行参数
 * 
 * @author guest
 *
 */
class CommandLineArgs {
	/**
	 * 源数据文件路径,如"hdfs://localhost:9000/hw1/orders.tbl"
	 */
	private static String sourceDataFilePath;
	/**
	 * 根据哪一行的值进行group By操作
	 * 如，java Hw1GrpX R=<file> groupby:R2 res:avg(R3),count,max(R4)，结果为2
	 * 
	 */
	private static int groupByLine;
	/**
	 * 根据哪一行的值进行group By操作
	 * 如，"java Hw1GrpX R=<file> groupby:R2 res:avg(R3),count,max(R4)"，结果为2
	 */
	private static String[] statisticsOperations;
	
	
	/**
	 * 获取第i个操作的操作对象是哪一列，i=0,1,...
	 * 如第0个操作字符串为"avg(R3)",则statisticsLinesNum[0]=3
	 * 
	 * 如果是count命令，则设对第0行操作
	 */
	private static int[] statisticsLinesNum;
	
	/**
	 * 主函数
	 * 
	 * 处理命令行参数
	 * 
	 * @param args 命令行参数
	 */
	public static void setCommandArgs(String[] args){
		setSourceDataFilePath(args[0].substring(2));
		setGroupByLine(args[1]);
		setStatisticsOperations(args[2].substring(4));
		setStatisticsLinesNum();
	}
	
	

	/**
	 * 获取源数据文件路径,如"hdfs://localhost:9000/hw1/orders.tbl"
	 * @return sourceDataFilePath 完整的源数据文件路径
	 */
	public static String getSourceDataFilePath() {
		return sourceDataFilePath;
	}

	/**
	 * 设置源数据文件路径,如"hdfs://localhost:9000/hw1/orders.tbl"
	 * @param partSourceDataFilePath 输入的部分源数据文件路径
	 */
	public static void setSourceDataFilePath(String partSourceDataFilePath) {
		sourceDataFilePath = "hdfs://localhost:9000"+partSourceDataFilePath;
	}

	/**	
	 * 获取根据哪一行的值进行group By操作
	 * 如，java Hw1GrpX R=<file> groupby:R2 res:avg(R3),count,max(R4)，结果为2
	 * @return groupByLine 根据这一行，进行group by操作
	 */
	public static int getGroupByLine() {
		return groupByLine;
	}

	/**
	 * 设置根据哪一行的值进行group By操作
	 * 如，java Hw1GrpX R=<file> groupby:R2 res:avg(R3),count,max(R4)，结果为2
	 * @param groupByLine 根据这一行，进行group by操作
	 */
	public static void setGroupByLine(String groupByLineString) {
		try{
			groupByLine =Integer.valueOf(groupByLineString.substring(9,10));
		}
		catch(Exception e){
			
		}
		
	}

	/**
	 * 获取对输入数据group by之后使用的的统计操作的字符串
	 * 如"res:count,avg(R3),max(R4)"可得statisticsOperations ={"count","avg(R3)","max(R4)"}
	 * @return statisticsOperations 对输入数据group by之后使用的的统计操作
	 */
	public static String[] getStatisticsOperations() {
		return statisticsOperations;
	}

	/**
	 * 设置对输入数据group by之后使用的的统计操作的字符串
	 * 如"res:count,avg(R3),max(R4)"可得statisticsOperations ={"count","avg(R3)","max(R4)"}
	 * @param statisticsOperations 对输入数据group by之后使用的的统计操作的字符串
	 * 
	 */
	public static void setStatisticsOperations(String input) {
		statisticsOperations = input.split(",");
	}

	/**
	 * 获取第i个操作的操作对象是哪一列，i=0,1,...
	 * 如第0个操作字符串为"avg(R3)",则statisticsLinesNum[0]=3
	 * @param i
	 * @return
	 */
	public static int getStatisticsColumnsNum(int i) {
		return statisticsLinesNum[i];
	}

	/**
	 * 设置第i个操作的操作对象是哪一列，i=0,1,...
	 * 如第0个操作字符串为"avg(R3)",则statisticsLinesNum[0]=3
	 */
	public static void setStatisticsLinesNum() {
		statisticsLinesNum=new int[getStatisticsOperations().length];
		for(int i=0;
				i<getStatisticsOperations().length;
				i++){
			try {
				statisticsLinesNum[i]=Integer.valueOf(
						getStatisticsOperations()[i].substring(
								getStatisticsOperations()[i].length()-2
								,getStatisticsOperations()[i].length()-1
								)
						);
			} catch (NumberFormatException e) {
				//若为count命令，其没有对应操作的列，则设为0
				statisticsLinesNum[i]=0;
			}
		}
	}
}
