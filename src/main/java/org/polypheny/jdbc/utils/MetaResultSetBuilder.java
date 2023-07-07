package org.polypheny.jdbc.utils;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyphenyBidirectionalResultSet;
import org.polypheny.jdbc.PolyphenyColumnMeta;
import org.polypheny.jdbc.proto.Namespace;
import org.polypheny.jdbc.proto.NamespacesResponse;
import org.polypheny.jdbc.proto.Table;
import org.polypheny.jdbc.proto.TableTypesResponse;
import org.polypheny.jdbc.proto.TablesResponse;
import org.polypheny.jdbc.types.TypedValue;

public class MetaResultSetBuilder {

    private static final TypedValue NULL_VARCHAR = TypedValue.fromNull( Types.VARCHAR );
    private static final int VARCHAR_NORMAL_MAXIMUM_WIDTH = 2147483647;


    private static ArrayList<PolyphenyColumnMeta> generateMetas( List<Integer> jdbcTypes, String entityName, String... columnLabels ) {
        ArrayList<PolyphenyColumnMeta> columnMetas = new ArrayList<>();
        for ( String label : columnLabels ) {
            int ordinal = columnMetas.size();
            columnMetas.add( PolyphenyColumnMeta.fromSpecification( ordinal, label, entityName, jdbcTypes.get( ordinal ) ) );
        }
        return columnMetas;
    }


    public static ResultSet buildFromTablesResponse( TablesResponse tablesResponse ) {
        ArrayList<ArrayList<TypedValue>> rows = new ArrayList<>();
        ArrayList<TypedValue> currentRow;
        for ( Table table : tablesResponse.getTablesList() ) {
            currentRow = new ArrayList<>();
            currentRow.add( TypedValue.fromString( table.getSourceDatabaseName() ) );
            currentRow.add( TypedValue.fromString( table.getNamespaceName() ) );
            currentRow.add( TypedValue.fromString( table.getTableName() ) );
            currentRow.add( TypedValue.fromString( table.getTableType() ) );
            currentRow.add( NULL_VARCHAR ); //REMARKS always null
            currentRow.add( NULL_VARCHAR ); //TYPE_CAT always null
            currentRow.add( NULL_VARCHAR ); //TYPE_SCHEM always null
            currentRow.add( NULL_VARCHAR ); //TYPE_NAME always null
            currentRow.add( NULL_VARCHAR ); //SELF_REFERENCING_COL_NAME always null
            currentRow.add( NULL_VARCHAR ); //REF_GENERATION always null
            currentRow.add( TypedValue.fromString( table.getOwnerName() ) );

            rows.add( currentRow );
        }
        ArrayList<PolyphenyColumnMeta> columnMetas = generateMetas(
                Collections.nCopies( 11, Types.VARCHAR ),
                "TABLES",
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION",
                "OWNER"
        );
        return new PolyphenyBidirectionalResultSet( columnMetas, rows );
    }


    public static ResultSet buildFromTableTypesResponse( TableTypesResponse tableTypesResponse ) {
        ArrayList<ArrayList<TypedValue>> rows = tableTypesResponse.getTableTypesList().stream()
                .map( TypedValue::fromString )
                .map( t -> new ArrayList<>( Arrays.asList( t ) ) )
                .collect( Collectors.toCollection( ArrayList::new ) );
        ArrayList<PolyphenyColumnMeta> columnMetas = generateMetas(
                Collections.singletonList( Types.VARCHAR ),
                "TABLE_TYPES",
                "TABLE_TYPE"
        );
        return new PolyphenyBidirectionalResultSet( columnMetas, rows );
    }


    public static ResultSet buildFromNamespacesResponse( NamespacesResponse namespacesResponse ) {
        ArrayList<ArrayList<TypedValue>> rows = new ArrayList<>();
        ArrayList<TypedValue> currentRow;
        for ( Namespace namespace : namespacesResponse.getNamespacesList() ) {
            currentRow = new ArrayList<>();
            currentRow.add( TypedValue.fromString( namespace.getNamespaceName() ) );
            currentRow.add( TypedValue.fromString( namespace.getDatabaseName() ) );
            currentRow.add( TypedValue.fromString( namespace.getOwnerName() ) );
            if ( namespace.hasNamespaceType() ) {
                currentRow.add( TypedValue.fromString( namespace.getNamespaceType() ) );
            } else {
                currentRow.add( NULL_VARCHAR );
            }
            rows.add( currentRow );
        }
        ArrayList<PolyphenyColumnMeta> columnMetas = generateMetas(
                Collections.nCopies( 4, Types.VARCHAR ),
                "NAMESPACES",
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_CATALOG",
                "OWNER",
                "SCHEMA_TYPE"
        );
        return new PolyphenyBidirectionalResultSet( columnMetas, rows );
    }

}
