package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import org.polypheny.jdbc.utils.PropertyUtils;

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

            /*
            Statement stmt = conn.createStatement();
            stmt.addBatch( "drop table test" );
            stmt.addBatch( "create table test (id int, name varchar(30))" );
            stmt.addBatch( "insert into test values(1, 'foo'),(2, 'bar')" );
            stmt.addBatch( "insert into test values(3, 'baz')" );
            int[] update_counts = stmt.executeBatch();
            System.out.println( Arrays.toString( update_counts ) );
            stmt.close();
            */

            PreparedStatement pstmt = conn.prepareStatement( "insert into test values(?, ?)" );
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }

}


