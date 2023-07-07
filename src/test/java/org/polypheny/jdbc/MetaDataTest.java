package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.jdbc.utils.PropertyUtils;

public class MetaDataTest {
    private final static String DB_URL = "jdbc:polypheny://localhost:20590";
    private final static String USER = "pa";
    private final static String PASS = "";
    static Connection dbConnection;

    final
    @BeforeClass
    public static void setUpClass() throws ClassNotFoundException, SQLException {
        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );final Properties CONNECTION_PROPERTIES = new Properties();
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getUSERNAME_KEY(), USER );
        CONNECTION_PROPERTIES.setProperty( PropertyUtils.getPASSWORD_KEY(), PASS );
        dbConnection = DriverManager.getConnection( DB_URL, CONNECTION_PROPERTIES);
    }


    @AfterClass
    public static void tearDownClass() throws SQLException {
        dbConnection.close();
    }


    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {
    }


    @Test
    public void getTables__NoError() throws Exception {
        ResultSet rs = dbConnection.getMetaData().getTables(null, null, "%",null );
        while (rs.next()) {
            System.out.println(rs.getString("TABLE_NAME"));
        }
    }

    @Test
    public void getTableTypes__NoError() throws Exception {
        ResultSet rs = dbConnection.getMetaData().getTableTypes();
        while (rs.next()) {
            System.out.println(rs.getString("TABLE_TYPE"));
        }
    }

}
