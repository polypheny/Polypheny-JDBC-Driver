package org.polypheny.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import org.junit.Test;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.Result.ResultType;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.proto.Document;

public class QueryTest {

    private static final String MQL_LANGUAGE_NAME = "mongo";
    private static final String TEST_DATA = "db.test.insertOne({name: \"John Doe\", age: 20, subjects: [\"Math\", \"Physics\", \"Chemistry\"], address: {street: \"123 Main St\", city: \"Anytown\", state: \"CA\", postalCode: \"12345\"}, graduationYear: 2023});";
    private static final String TEST_QUERY = "db.emps.find({});";


    @Test
    public void thisOneWorks() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try (
                Connection connection = DriverManager.getConnection( "jdbc:polypheny://localhost:20590" );
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery( "SELECT * FROM emps" );
        ) {
            while ( resultSet.next() ) {
                // Process the result set...
            }
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }


    @Test
    public void simpleMqlTest() throws ClassNotFoundException {

        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try ( Connection connection = DriverManager.getConnection( DB_URL, USER, PASS ) ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createProtoStatement();
            Result result = polyStatement.execute( "public", MQL_LANGUAGE_NAME, TEST_QUERY );
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    @Test
    public void mqlDataRetrievalTest() throws ClassNotFoundException {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try ( Connection connection = DriverManager.getConnection( DB_URL, USER, PASS ) ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createProtoStatement();
            Result result = polyStatement.execute( "public", MQL_LANGUAGE_NAME, TEST_QUERY );
            DocumentResult docs = result.unwrap( DocumentResult.class );
            Iterator<PolyDocument> iterator = docs.iterator();
            while ( iterator.hasNext() ) {
                System.out.println(iterator.next().toString());
            }
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

}

