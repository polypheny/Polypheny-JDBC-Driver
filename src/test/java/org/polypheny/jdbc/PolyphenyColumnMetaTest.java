package org.polypheny.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.jdbc.proto.ColumnMeta;
import org.polypheny.jdbc.proto.ProtoValueType;
import org.polypheny.jdbc.proto.TypeMeta;
import org.polypheny.jdbc.types.ProtoToJdbcTypeMap;
import org.polypheny.jdbc.types.ProtoToPolyTypeNameMap;

public class PolyphenyColumnMetaTest {

    private static ColumnMeta protoColumnMeta;
    private static ColumnMeta negatedColumnMeta;
    private static PolyphenyColumnMeta specifiedColumnMeta;

    // parameters used to generate test data
    private static final boolean IS_NULLABLE = true;
    private static final int COLUMN_INDEX = 42;
    private static final int LENGTH = 43;
    private static final int PRECISION = 44;
    private static final int SCALE = 44;
    private static final String COLUMN_LABEL = "COLUMN_LABEL";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String ENTITY_NAME = "ENTITY_NAME";
    private static final String SCHEMA_NAME = "SCHEMA_NAME";
    private static final String NAMESPACE_NAME = "NAMESPACE_NAME";
    private static final ProtoValueType VALUE_TYPE = ProtoValueType.PROTO_VALUE_TYPE_BIGINT;
    private static final int JDBC_TYPE = Types.NCLOB;


    @BeforeClass
    public static void setUpClass() {
        TypeMeta typeMeta = TypeMeta.newBuilder()
                .setProtoValueType( VALUE_TYPE )
                .build();
        protoColumnMeta = ColumnMeta.newBuilder()
                .setColumnIndex( COLUMN_INDEX )
                .setIsNullable( IS_NULLABLE )
                .setLength( LENGTH )
                .setColumnLabel( COLUMN_LABEL )
                .setColumnName( COLUMN_NAME )
                .setPrecision( PRECISION )
                .setEntityName( ENTITY_NAME )
                .setSchemaName( SCHEMA_NAME )
                .setTypeMeta( typeMeta )
                .setScale( SCALE )
                .setNamespace( NAMESPACE_NAME )
                .build();
        negatedColumnMeta = ColumnMeta.newBuilder()
                .setColumnIndex( COLUMN_INDEX )
                .setIsNullable( !IS_NULLABLE )
                .setLength( LENGTH )
                .setColumnLabel( COLUMN_LABEL )
                .setColumnName( COLUMN_NAME )
                .setPrecision( PRECISION )
                .setEntityName( ENTITY_NAME )
                .setSchemaName( SCHEMA_NAME )
                .setTypeMeta( typeMeta )
                .setScale( SCALE )
                .setNamespace( NAMESPACE_NAME )
                .build();
        specifiedColumnMeta = PolyphenyColumnMeta.fromSpecification( COLUMN_INDEX, COLUMN_LABEL, ENTITY_NAME, JDBC_TYPE );
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
    public void protoConstructor__ColumnMeta_Ordinal() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( COLUMN_INDEX, meta.getOrdinal() );
    }


    @Test
    public void protoConstructor__ColumnMeta_AutoIncrement() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isAutoIncrement() );
    }


    @Test
    public void protoConstructor__ColumnMeta_CaseSensitive() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertTrue( meta.isCaseSensitive() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Searchable() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isSearchable() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Currency() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isCurrency() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Nullable() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( ResultSetMetaData.columnNullable, meta.getNullable() );

        PolyphenyColumnMeta negatedMeta = new PolyphenyColumnMeta( negatedColumnMeta );
        assertEquals( ResultSetMetaData.columnNoNulls, negatedMeta.getNullable() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Signed() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isSigned() );
    }


    @Test
    public void protoConstructor__ColumnMeta_DisplaySize() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( LENGTH, meta.getDisplaySize() );
    }


    @Test
    public void protoConstructor__ColumnMeta_ColumnLabel() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( COLUMN_LABEL, meta.getColumnLabel() );
    }


    @Test
    public void protoConstructor__ColumnMeta_ColumnName() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( COLUMN_NAME, meta.getColumnName() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Namespace() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( NAMESPACE_NAME, meta.getNamespace() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Precision() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( PRECISION, meta.getPrecision() );
    }


    @Test
    public void protoConstructor__ColumnMeta_Scale() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( 1, meta.getScale() );
    }


    @Test
    public void protoConstructor__ColumnMeta_TableName() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( ENTITY_NAME, meta.getTableName() );
    }


    @Test
    public void protoConstructor__ColumnMeta_CatalogName() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( "", meta.getCatalogName() );
    }


    @Test
    public void protoConstructor__ColumnMeta_ReadOnly() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isReadOnly() );

    }


    @Test
    public void protoConstructor__ColumnMeta_Writable() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isWritable() );
    }


    @Test
    public void protoConstructor__ColumnMeta_DefinitelyWritable() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertFalse( meta.isDefinitelyWritable() );
    }


    @Test
    public void protoConstructor__ColumnMeta_ColumnClassName() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        assertEquals( "", meta.getColumnClassName() );
    }


    @Test
    public void protoConstructor__ColumnMeta_SqlType() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        int expected = ProtoToJdbcTypeMap.getJdbcTypeFromProto( VALUE_TYPE );
        assertEquals( expected, meta.getSqlType() );
    }


    @Test
    public void protoConstructor__ColumnMeta_FieldTypeName() {
        PolyphenyColumnMeta meta = new PolyphenyColumnMeta( protoColumnMeta );
        String expected = ProtoToPolyTypeNameMap.getPolyTypeNameFromProto( VALUE_TYPE );
        assertEquals( expected, meta.getPolyphenyFieldTypeName() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Ordinal() {
        assertEquals( COLUMN_INDEX, specifiedColumnMeta.getOrdinal() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_AutoIncrement() {
        assertFalse( specifiedColumnMeta.isAutoIncrement() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_CaseSensitive() {
        assertTrue( specifiedColumnMeta.isCaseSensitive() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Searchable() {
        assertFalse( specifiedColumnMeta.isSearchable() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Currency() {
        assertFalse( specifiedColumnMeta.isCurrency() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Nullable() {
        assertEquals( ResultSetMetaData.columnNullable, specifiedColumnMeta.getNullable() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Signed() {
        assertFalse( specifiedColumnMeta.isSigned() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_DisplaySize() {
        assertEquals( -1, specifiedColumnMeta.getDisplaySize() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_ColumnLabel() {
        assertEquals( COLUMN_LABEL, specifiedColumnMeta.getColumnLabel() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_ColumnName() {
        assertNull(specifiedColumnMeta.getColumnName() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Namespace() {
        assertNull(specifiedColumnMeta.getNamespace() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Precision() {
        assertEquals( -1, specifiedColumnMeta.getPrecision() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Scale() {
        assertEquals( 1, specifiedColumnMeta.getScale() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_TableName() {
        assertEquals( ENTITY_NAME, specifiedColumnMeta.getTableName() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_CatalogName() {
        assertEquals( "", specifiedColumnMeta.getCatalogName() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_ReadOnly() {
        assertFalse( specifiedColumnMeta.isReadOnly() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_Writable() {

        assertFalse( specifiedColumnMeta.isWritable() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_DefinitelyWritable() {

        assertFalse( specifiedColumnMeta.isDefinitelyWritable() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_ColumnClassName() {

        assertEquals( "", specifiedColumnMeta.getColumnClassName() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_SqlType() {
        assertEquals( JDBC_TYPE, specifiedColumnMeta.getSqlType() );
    }


    @Test
    public void fromSpecification__Ordinal_ColumnName_EntityName_JdbcType_FieldTypeName() {
        assertEquals( "", specifiedColumnMeta.getPolyphenyFieldTypeName() );
    }

}
