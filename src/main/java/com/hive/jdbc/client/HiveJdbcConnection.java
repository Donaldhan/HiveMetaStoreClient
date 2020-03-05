package com.hive.jdbc.client;

import groovy.util.logging.Slf4j;
import org.mortbay.log.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * @author revanthreddy 
 * Basic example to test the hiveserver2 connection 
 * 使用前，启动hiveserver2
 */
@Slf4j
public class HiveJdbcConnection {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws Exception {
		try {

			Class.forName(driverName);
			Connection connection = null;
			System.out.println("Before getting connection");
			connection = DriverManager.getConnection("jdbc:hive2://pseduoDisHadoop:10000/test", "hadoop", "123456");
			System.out.println("After getting connection " + connection);

			ResultSet resultSet = connection.createStatement().executeQuery("select * from emp");
            //遍历查询结果
			while (resultSet.next()) {
				Log.info("name : {}, age :{}", resultSet.getString(1) , resultSet.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}

	}
}
