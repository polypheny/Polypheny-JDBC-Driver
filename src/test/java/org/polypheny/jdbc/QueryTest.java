package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.jdbc.utils.PropertyUtils;

public class QueryTest {

    @BeforeClass
    public static void setUpClass() {
    }


    @AfterClass
    public static void tearDownClass() {
    }


    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {
    }


    @Test
    public void selectStar__NoError() throws Exception {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";
        final String QUERY = "SELECT * FROM emp";

        final Properties CONNECTION_PROPERTIES = new Properties();
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getUSERNAME_KEY(), USER );
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getPASSWORD_KEY(), PASS );

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try ( Connection conn = DriverManager.getConnection( DB_URL, CONNECTION_PROPERTIES );
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery( QUERY )
        ) {
            while ( rs.next() ) {
                System.out.print( "ID: " + rs.getInt( "employeeno" ) );
                System.out.print( ", Age: " + rs.getInt( "age" ) );
            }
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }

}
