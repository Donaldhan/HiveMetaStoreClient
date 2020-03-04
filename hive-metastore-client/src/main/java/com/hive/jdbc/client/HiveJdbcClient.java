package com.hive.jdbc.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
/**
 * @author revanthreddy
 * 使用前，启动hiveserver2
 */
import java.sql.SQLException;
import java.sql.Statement;

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
		String query = "CREATE EXTERNAL TABLE IF NOT EXISTS employee ( eid int, name String,dept String, yoj String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' ";
//		String query = "create TABLE student (id int,name string,tel string,age int) row format delimited fields terminated by '\\t';";

		stmt.execute(query);

		// show tables
		String sql = "show tables";
		System.out.println("Running Query : " + sql);
		ResultSet res1 = stmt.executeQuery(sql);
		if (res1.next()) {
			System.out.println(res1.getString(1));
		}

		// describe table
		sql = "describe " + tableName;
		System.out.println("Running Query : " + sql);
		res1 = stmt.executeQuery(sql);
		while (res1.next()) {
			System.out.println(res1.getString(1) + "\t" + res1.getString(2));
		}

		// load data into table , 这里使用的是本地文件加载数据方式
		String filepath = "/bdp/hive/hiveLocalTables/student.txt";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running Query : " + sql);
		stmt.execute(sql);

		// select * query
		String sqlquery = "select * from " + tableName;
		System.out.println("Running Query : " + sqlquery);
		ResultSet res2 = stmt.executeQuery(sqlquery);
		while (res2.next()) {
			System.out.println(String.valueOf(res2.getInt(1)) + "\t" + res2.getString(2));
		}

	}
}
