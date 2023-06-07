package org.polypheny.jdbc.test;

import java.io.IOException;

public class TestLauncher {

    public static void main( String[] args ) {
        try {
            TestClient testClient = new TestClient();
            TestServer testServer = new TestServer();
            new Thread( testClient::callA ).start();
            Thread.sleep(4000);
            new Thread( testClient::callB ).start();
        } catch ( RuntimeException | IOException re ) {
            System.out.println( re.getMessage() );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }

}
