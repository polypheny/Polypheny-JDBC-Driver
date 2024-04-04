/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

class TestHelper implements BeforeAllCallback, AfterAllCallback {

    private static Process p = null;


    private TestHelper() {
    }


    @Override
    public void beforeAll( ExtensionContext context ) {
        String java = System.getenv( "POLYPHENY_JAVA" );
        String jar = System.getenv( "POLYPHENY_JAR" );
        if ( java == null ) {
            java = "java";
        }
        if ( jar == null ) {
            return;
        }
        try {
            p = new ProcessBuilder( java, "-jar", jar, "-resetCatalog", "-resetDocker" ).start();
            try ( BufferedReader in = new BufferedReader( new InputStreamReader( p.getInputStream() ) ) ) {
                List<String> lines = new ArrayList<>();
                while ( true ) {
                    String line = in.readLine();
                    if ( line == null ) {
                        try ( BufferedReader err = new BufferedReader( new InputStreamReader( p.getErrorStream() ) ) ) {
                            line = err.readLine();
                            while ( line != null ) {
                                System.err.println( line );
                                line = err.readLine();
                            }
                        }
                        System.out.println( String.join( "\n", lines ) );
                        throw new RuntimeException( "Unexpected EOF" );
                    }
                    if ( !line.isEmpty() ) {
                        lines.add( line );
                    }
                    if ( line.contains( "Polypheny-DB successfully started" ) ) {
                        break;
                    }
                }
            }
        } catch ( IOException e ) {
            throw new RuntimeException( "Could not start Polypheny: ", e );
        }
    }


    @Override
    public void afterAll( ExtensionContext context ) {
        if ( p != null ) {
            p.destroyForcibly();
        }
    }

}
