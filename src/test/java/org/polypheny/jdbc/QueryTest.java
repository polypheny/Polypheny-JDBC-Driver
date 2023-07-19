package org.polypheny.jdbc;

import org.junit.Test;
import org.polypheny.jdbc.properties.PropertyUtils;

import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;

public class QueryTest {

    public static void main(String[] args) throws ClassNotFoundException {


    }

    @Test
    public void thisOneWorks() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName("org.polypheny.jdbc.PolyphenyDriver");

        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS my_next_table (id INT, name VARCHAR(50))";
            Statement statement = connection.createStatement();
            statement.execute(createTableSQL);

            String insertSQL = "INSERT INTO my_next_table (id, name) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);

            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "John Doe");

            System.out.println("Values inserted successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void parserCrashesOnValidStatementOnCreateTable() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName("org.polypheny.jdbc.PolyphenyDriver");

        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS my_other_table (id INT, name VARCHAR(50), value NUMERIC)";
            connection.createStatement().execute(createTableSQL);

            String insertSQL = "INSERT INTO my_other_table (id, name, value) VALUES (?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);

            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "John Doe");
            preparedStatement.setObject(3, new BigInteger("123456789012345678901234567890"));
            preparedStatement.executeUpdate();

            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "Jane Smith");
            preparedStatement.setObject(3, new BigInteger("987654321098765432109876543210"));
            preparedStatement.executeUpdate();

            preparedStatement.setInt(1, 3);
            preparedStatement.setString(2, "Alice Johnson");
            preparedStatement.setObject(3, new BigInteger("555555555555555555555555555555"));
            preparedStatement.executeUpdate();

            System.out.println("Values inserted successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void failsWithNullPointerExceptionInPolyImplementation() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName("org.polypheny.jdbc.PolyphenyDriver");

        try (Connection connection = DriverManager.getConnection(DB_URL, USER, PASS)) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS my_table (id INT, name VARCHAR(50))";
            Statement statement = connection.createStatement();
            statement.execute(createTableSQL);

            String insertSQL = "INSERT INTO my_table (id, name) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);

            preparedStatement.setInt(1, 1);
            preparedStatement.setString(2, "John Doe");
            preparedStatement.executeUpdate();

            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "Jane Smith");
            preparedStatement.executeUpdate();

            preparedStatement.setInt(1, 3);
            preparedStatement.setString(2, "Alice Johnson");
            preparedStatement.executeUpdate();

            System.out.println("Values inserted successfully!");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

