package com.practice.kafka.consumer;

import java.sql.*;

public class JDBCTester {

    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        String url = "jdbc:postgresql://192.168.56.101:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        try {
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT 'postgresql is connected'");

            if (resultSet.next())
                System.out.println(resultSet.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }finally{
            try {
                resultSet.close();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
