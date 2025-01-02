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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.GraphResult;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalMetadata;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.types.PolyDocument;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.types.TypedValue;

public class Demo {

    private static String language = "sql";
    private static String namespace = "public";
    private static Connection con;


    public static void main( String[] args ) {
        try {
            con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590?strict=false", "pa", "" );
            if ( !con.isWrapperFor( PolyConnection.class ) ) {
                System.out.println( "Driver must support unwrapping to PolyConnection" );
                return;
            }
            Scanner scanner = new Scanner( System.in );

            while ( true ) {
                printPrompt();
                String input = scanner.nextLine();

                if ( input.startsWith( "lng " ) ) {
                    language = input.substring( 4 ).trim();
                    System.out.println( "Language set to " + language );
                } else if ( input.startsWith( "ns " ) ) {
                    namespace = input.substring( 3 ).trim();
                    System.out.println( "Namespace set to " + namespace );
                } else if ( input.equals( "exit" ) ) {
                    break;
                } else {
                    if ( language == null || namespace == null ) {
                        System.out.println( "Please set both language and namespace before executing a statement." );
                        continue;
                    }
                    String statement = input.trim();
                    executeStatement( namespace, language, statement );
                }
            }

            scanner.close();
        } catch ( Exception e ) {
            e.printStackTrace();
        } finally {
            try {
                if ( con != null && !con.isClosed() ) {
                    con.close();
                }
            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }
    }


    private static void printPrompt() {
        System.out.print( language + "@" + namespace + "> " );
    }


    private static void executeStatement( String namespace, String language, String statement ) {
        try ( Connection con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590?strict=false", "pa", "" ) ) {
            if ( !con.isWrapperFor( PolyConnection.class ) ) {
                System.out.println( "Driver must support unwrapping to PolyConnection" );
                return;
            }
            PolyStatement polyStatement = con.unwrap( PolyConnection.class ).createPolyStatement();
            Result result = polyStatement.execute( namespace, language, statement );
            switch ( result.getResultType() ) {
                case RELATIONAL:
                    RelationalResult relationalResult = result.unwrap( RelationalResult.class );
                    printRelationalResult( relationalResult );
                    break;
                case DOCUMENT:
                    DocumentResult documentResult = result.unwrap( DocumentResult.class );
                    printDocumentResult( documentResult );
                    break;
                case SCALAR:
                    ScalarResult scalarResult = result.unwrap( ScalarResult.class );
                    System.out.println( "Update count: " + scalarResult.getScalar() + "." );
                    break;
                case GRAPH:
                    GraphResult graphResult = result.unwrap( GraphResult.class );
                    printGraphResult( graphResult );
                    break;
                default:
                    System.out.println( "Unknown result type." );
            }
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }


    private static void printRelationalResult( RelationalResult relationalResult ) throws PrismInterfaceServiceException {
        RelationalMetadata metadata = relationalResult.getMetadata();
        int columnsCount = metadata.getColumnCount();

        List<String> columnLabels = new ArrayList<>();
        List<Integer> columnWidths = new ArrayList<>();
        for ( int i = 0; i < columnsCount; i++ ) {
            String label = metadata.getColumnMeta( i ).getColumnLabel();
            columnLabels.add( label );
            columnWidths.add( label.length() );
        }

        List<List<String>> rows = new ArrayList<>();
        for ( PolyRow row : relationalResult ) {
            List<String> formattedRow = new ArrayList<>();
            for ( int colIndex = 0; colIndex < columnsCount; colIndex++ ) {
                String valueStr = row.get( colIndex ).toString();
                formattedRow.add( valueStr );
                columnWidths.set( colIndex, Math.max( columnWidths.get( colIndex ), valueStr.length() ) );
            }
            rows.add( formattedRow );
        }

        printSeparator( columnWidths );
        printRow( columnLabels, columnWidths );
        printSeparator( columnWidths );
        for ( List<String> row : rows ) {
            printRow( row, columnWidths );
        }
        printSeparator( columnWidths );
    }


    private static void printDocumentResult( DocumentResult documentResult ) {
        System.out.println( "DOC------------------------" );
        for ( PolyDocument document : documentResult ) {
            System.out.println( document.toString() );
            System.out.println( "---------------------------" );
        }
    }


    private static void printGraphResult( GraphResult graphResult ) {
        System.out.println( "GRAPH----------------------" );
        for ( PolyGraphElement graphElement : graphResult ) {
            for ( Map.Entry<String, TypedValue> property : graphElement.entrySet() ) {
                System.out.println( property.getKey() + ": " + property.getValue() );
            }
            System.out.println( "---------------------------" );
        }
    }


    private static void printSeparator( List<Integer> columnWidths ) {
        for ( int width : columnWidths ) {
            System.out.print( "+" );
            for ( int i = 0; i < width + 2; i++ ) {
                System.out.print( "-" );
            }
        }
        System.out.println( "+" );
    }


    private static void printRow( List<String> row, List<Integer> columnWidths ) {
        for ( int i = 0; i < row.size(); i++ ) {
            System.out.print( "| " + padRight( row.get( i ), columnWidths.get( i ) ) + " " );
        }
        System.out.println( "|" );
    }


    private static String padRight( String s, int n ) {
        return String.format( "%-" + n + "s", s );
    }

}
