package dbtest;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.RandomStringUtils;


public class DbTest {

	public static void main(String[] args) {
		String url = "jdbc:mysql://192.168.0.99:3306/MyTest";
		String username = "root";
		String password = "asdasADWFA213asdwa#$%#1s";
		
		int size = 64;
		
		System.out.println("Connecting database...");

		try (Connection connection = DriverManager.getConnection(url, username, password)) {
		    System.out.println("Database connected!");
		    
		    Statement stmt = connection.createStatement();

//		   String data = RandomStringUtils.randomAlphabetic(256);
		    
		    for (int i = 0; i < 10_000_000; i++) {
		    	
		    	stmt.execute("INSERT INTO `MyTest`.`MyTable` (`field_one`, `field_two`, `field_three`, `field_four`) VALUES ('" + 
		    	RandomStringUtils.randomAlphabetic(size) + "', '" + 
		    			RandomStringUtils.randomAlphabetic(size) + "', '" + 
		    	RandomStringUtils.randomAlphabetic(size) + "', '" + RandomStringUtils.randomAlphabetic(size) + "');");
			}
		    stmt.close();		   
		} catch (SQLException e) {
		    throw new IllegalStateException("Cannot connect the database!", e);
		}
	}
}
