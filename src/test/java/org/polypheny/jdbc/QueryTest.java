package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
            Statement stmt = conn.createStatement();
            ResultSet rs;
            if ( stmt.execute( QUERY ) ) {
                rs = stmt.getResultSet();
                int count = rs.getMetaData().getColumnCount();
                while ( rs.next() ) {
                    System.out.print( "ID: " + rs.getInt( "employeeno" ) );
                    System.out.print( ", Age: " + rs.getInt( "age" ) + "\n" );
                }
            }
            int uc = stmt.getUpdateCount();
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }
}


