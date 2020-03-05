package com.hive.jdbc.client;



import groovy.util.logging.Slf4j;
import org.mortbay.log.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
/**
 * @author revanthreddy
 * 使用前，启动hiveserver2
 */
@Slf4j
public class HiveJdbcClient {
	private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive2://pseduoDisHadoop:10000/test", "hadoop", "123456");
		Statement stmt = con.createStatement();

		String tableName = "student";
		stmt.execute("DROP TABLE " + tableName);
		String query = "CREATE TABLE student (id int,name string,tel string,age int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'";

		stmt.execute(query);

		// show tables
		String sql = "show tables";
		Log.info("Running Query : {}" ,sql);
		ResultSet res1 = stmt.executeQuery(sql);
		if (res1.next()) {
			System.out.println(res1.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		Log.info("Running Query : {}" + sql);
		res1 = stmt.executeQuery(sql);
		while (res1.next()) {
			System.out.println(res1.getString(1) + "\t" + res1.getString(2));
		}

		// load data into table , 这里使用的是本地文件加载数据方式
		String filepath = "/bdp/hive/hiveLocalTables/student.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		Log.info("Running Query :{} " ,sql);
		stmt.execute(sql);

		// select * query
		String sqlquery = "select * from " + tableName;
		Log.info("Running Query :{} " ,sqlquery);
		ResultSet res2 = stmt.executeQuery(sqlquery);
		while (res2.next()) {
			Log.info(String.valueOf(res2.getInt(1)) + "\t" + res2.getString(2));
		}

	}
}
