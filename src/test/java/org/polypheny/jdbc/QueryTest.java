package org.polypheny.jdbc;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class QueryTest {

    @Test
    public void thisOneWorks() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try ( Connection connection = DriverManager.getConnection( DB_URL, USER, PASS ) ) {

            String createTableSQL = "CREATE TABLE my_table (column1 INT NOT NULL, column2 VARCHAR(255), PRIMARY KEY (column1))";
            Statement statement = connection.createStatement();
            statement.executeUpdate( createTableSQL );

            String insertSQL = "INSERT INTO my_table (column1, column2) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement( insertSQL );

            for ( int i = 1; i <= 100; i++ ) {
                String hexValue = Integer.toHexString( i ).toUpperCase();
                preparedStatement.setInt( 1, i );
                preparedStatement.setString( 2, hexValue );
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            System.out.println( "Values inserted successfully!" );

        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }
}

