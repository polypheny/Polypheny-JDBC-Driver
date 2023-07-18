package org.polypheny.jdbc;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import org.polypheny.jdbc.properties.PropertyUtils;

public class QueryTest {

    public static void main( String[] args ) throws ClassNotFoundException {

        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";
        final String QUERY = "SELECT * FROM emp";

        final Properties CONNECTION_PROPERTIES = new Properties();
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getUSERNAME_KEY(), USER );
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getPASSWORD_KEY(), PASS );

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try {
            Connection conn = DriverManager.getConnection( DB_URL, CONNECTION_PROPERTIES );

            //This is not related to the bug. Just ignore...

            //Statement stmt = conn.createStatement();
            //stmt.execute("this is not sql");
            //stmt.execute("insert into test values(5, baum)");
            /*
            stmt.addBatch( "create table test (id int, name varchar(30))" );
            stmt.addBatch( "insert into test values(1, 'foo'),(2, 'bar')" );
            stmt.addBatch( "insert into test values(3, 'baz')" );
            int[] update_counts = stmt.executeBatch();
            System.out.println( Arrays.toString( update_counts ) );
            */
            //stmt.close();



            PolyphenyPreparedStatement pstmt = (PolyphenyPreparedStatement) conn.prepareStatement( "INSERT INTO public.test (a, b) VALUES (?, ?)" );
            pstmt.setInt(1, 42);
            pstmt.setInt(2, 69);
            //(42.0, '2023-07-21', 69.0, 56789.0);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }

}

