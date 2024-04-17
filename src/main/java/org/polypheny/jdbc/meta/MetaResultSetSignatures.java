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

package org.polypheny.jdbc.meta;

import java.sql.DatabaseMetaData;
import java.sql.PseudoColumnUsage;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.ObjectUtils;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMeta;
import org.polypheny.db.protointerface.proto.Column;
import org.polypheny.db.protointerface.proto.Namespace;
import org.polypheny.db.protointerface.proto.Procedure;
import org.polypheny.db.protointerface.proto.Table;
import org.polypheny.db.protointerface.proto.TableType;
import org.polypheny.db.protointerface.proto.Type;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class MetaResultSetSignatures {

    // Used as a placeholder for accessors in empty result sets
    private static final Function DUMMY_ACCESSOR = a -> "Dummy value: Accessor not implemented";


    public static final List<MetaResultSetParameter<Table>> TABLE_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, Table::getNamespaceName ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, Table::getTableName ),
            new MetaResultSetParameter<>( "TABLE_TYPE", Types.VARCHAR, Table::getTableType ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, p -> "" ),
            new MetaResultSetParameter<>( "TYPE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TYPE_SCHEM", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "SELF_REFERENCING_COL_NAME", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "REF_GENERATION", Types.VARCHAR, p -> null )
    );

    public static final List<MetaResultSetParameter<TableType>> TABLE_TYPE_SIGNATURE = Collections.singletonList(
            new MetaResultSetParameter<>( "TABLE_TYPE", Types.VARCHAR, TableType::getTableType )
    );

    public static final List<MetaResultSetParameter<Namespace>> NAMESPACE_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, Namespace::getNamespaceName ),
            new MetaResultSetParameter<>( "TABLE_CATALOG", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "SCHEMA_TYPE", Types.VARCHAR, nullIfFalse( Namespace::getNamespaceType, Namespace::hasNamespaceType ) )
    );

    public static final List<MetaResultSetParameter<Column>> COLUMN_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, Column::getNamespaceName ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, Column::getTableName ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, Column::getColumnName ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName( p.getTypeName() ) ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, Column::getTypeName ),
            new MetaResultSetParameter<>( "COLUMN_SIZE", Types.INTEGER, nullIfFalse( Column::getTypeLength, Column::hasTypeLength ) ),
            new MetaResultSetParameter<>( "BUFFER_LENGTH", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "DECIMAL_DIGITS", Types.INTEGER, nullIfFalse( convertScale( Column::getTypeScale ), Column::hasTypeScale ) ),
            new MetaResultSetParameter<>( "NUM_PREC_RADIX", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "NULLABLE", Types.INTEGER, p -> p.getIsNullable() ? 1 : 0 ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, p -> "" ),
            new MetaResultSetParameter<>( "COLUMN_DEF", Types.VARCHAR, nullIfFalse( Column::getDefaultValueAsString, Column::hasDefaultValueAsString ) ),
            new MetaResultSetParameter<>( "SQL_DATA_TYPE", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "SQL_DATETIME_SUB", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "CHAR_OCTET_LENGTH", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "ORDINAL_POSITION", Types.INTEGER, Column::getColumnIndex ),
            new MetaResultSetParameter<>( "IS_NULLABLE", Types.VARCHAR, p -> p.getIsNullable() ? "YES" : "NO" ),
            new MetaResultSetParameter<>( "SCOPE_CATALOG", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "SCOPE_SCHEMA", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "SCOPE_TABLE", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "SOURCE_DATA_TYPE", Types.SMALLINT, p -> null ),
            new MetaResultSetParameter<>( "IS_AUTOINCREMENT", Types.VARCHAR, p -> "No" ),
            new MetaResultSetParameter<>( "IS_GENERATEDCOLUMN", Types.VARCHAR, p -> "No" ),
            new MetaResultSetParameter<>( "COLLATION", Types.VARCHAR, nullIfFalse( Column::getCollation, Column::hasCollation ) )
    );

    public static final List<MetaResultSetParameter<GenericMetaContainer>> PRIMARY_KEY_GMC_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 0 ) ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, p -> p.getValue( 1 ) ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, p -> p.getValue( 2 ) ),
            new MetaResultSetParameter<>( "KEY_SEQ", Types.SMALLINT, p -> p.getValue( 3 ) ),
            new MetaResultSetParameter<>( "PK_NAME", Types.VARCHAR, p -> p.getValue( 4 ) )
    );

    // This signature uses the term catalog as this is what jdbc calls the results in the result set generated.
    public static final List<MetaResultSetParameter<String>> CATALOG_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, s -> "APP" ),
            new MetaResultSetParameter<>( "OWNER", Types.VARCHAR, s -> "system" ),
            new MetaResultSetParameter<>( "DEFAULT_SCHEMA", Types.VARCHAR, s -> s )
    );

    public static final List<MetaResultSetParameter<GenericMetaContainer>> FOREIGN_KEY_GMC_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "PKTABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "PKTABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 0 ) ),
            new MetaResultSetParameter<>( "PKTABLE_NAME", Types.VARCHAR, p -> p.getValue( 1 ) ),
            new MetaResultSetParameter<>( "PKCOLUMN_NAME", Types.VARCHAR, p -> null ),  // TODO: This is not standard compliant!
            new MetaResultSetParameter<>( "FKTABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "FKTABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 2 ) ),
            new MetaResultSetParameter<>( "FKTABLE_NAME", Types.VARCHAR, p -> p.getValue( 3 ) ),
            new MetaResultSetParameter<>( "FKCOLUMN_NAME", Types.VARCHAR, p -> p.getValue( 4 ) ),
            new MetaResultSetParameter<>( "KEY_SEQ", Types.SMALLINT, p -> p.getValue( 5 ) ),
            new MetaResultSetParameter<>( "UPDATE_RULE", Types.SMALLINT, p -> p.getValue( 6 ) ),
            new MetaResultSetParameter<>( "DELETE_RULE", Types.SMALLINT, p -> p.getValue( 7 ) ),
            new MetaResultSetParameter<>( "FK_NAME", Types.VARCHAR, p -> p.getValue( 8 ) ),
            new MetaResultSetParameter<>( "PK_NAME", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "DEFERRABILITY", Types.SMALLINT, p -> null )
    );

    public static final List<MetaResultSetParameter<Type>> TYPE_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, Type::getTypeName ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, t -> TypedValueUtils.getJdbcTypeFromPolyTypeName( t.getTypeName() ) ),
            new MetaResultSetParameter<>( "PRECISION", Types.INTEGER, Type::getPrecision ),
            new MetaResultSetParameter<>( "LITERAL_PREFIX", Types.VARCHAR, nullIfFalse( Type::getLiteralPrefix, Type::hasLiteralPrefix ) ),
            new MetaResultSetParameter<>( "LITERAL_SUFFIX", Types.VARCHAR, nullIfFalse( Type::getLiteralSuffix, Type::hasLiteralSuffix ) ),
            new MetaResultSetParameter<>( "CREATE_PARAMS", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "NULLABLE", Types.SMALLINT, p -> DatabaseMetaData.typeNullable ),
            new MetaResultSetParameter<>( "CASE_SENSITIVE", Types.BOOLEAN, Type::getIsCaseSensitive ),
            new MetaResultSetParameter<>( "SEARCHABLE", Types.SMALLINT, integerAsShort( Type::getIsSearchable ) ),
            new MetaResultSetParameter<>( "UNSIGNED_ATTRIBUTE", Types.BOOLEAN, p -> false ),
            new MetaResultSetParameter<>( "FIXED_PREC_SCALE", Types.BOOLEAN, p -> false ),
            new MetaResultSetParameter<>( "AUTO_INCREMENT", Types.BOOLEAN, Type::getIsAutoIncrement ),
            new MetaResultSetParameter<>( "LOCAL_TYPE_NAME", Types.VARCHAR, Type::getTypeName ),
            new MetaResultSetParameter<>( "MINIMUM_SCALE", Types.SMALLINT, convertScale( Type::getMinScale ) ),
            new MetaResultSetParameter<>( "MAXIMUM_SCALE", Types.SMALLINT, convertScale( Type::getMaxScale ) ),
            new MetaResultSetParameter<>( "SQL_DATA_TYPE", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "SQL_DATETIME_SUB", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "NUM_PREC_RADIX", Types.INTEGER, Type::getRadix )
    );

    public static final List<MetaResultSetParameter<GenericMetaContainer>> INDEX_GMC_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 0 ) ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, p -> p.getValue( 1 ) ),
            new MetaResultSetParameter<>( "NON_UNIQUE", Types.BOOLEAN, p -> p.getValue( 2 ) ),
            new MetaResultSetParameter<>( "INDEX_QUALIFIER", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "INDEX_NAME", Types.VARCHAR, p -> p.getValue( 3 ) ),
            new MetaResultSetParameter<>( "TYPE", Types.TINYINT, p -> 0 ),
            new MetaResultSetParameter<>( "ORDINAL_POSITION", Types.TINYINT, integerAsShort( p -> p.getValue( 4 ) ) ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, p -> p.getValue( 5 ) ),
            new MetaResultSetParameter<>( "ASC_OR_DESC", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "CARDINALITY", Types.BIGINT, p -> (long) -1 ),
            new MetaResultSetParameter<>( "PAGES", Types.BIGINT, p -> null ),
            new MetaResultSetParameter<>( "FILTER_CONDITION", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "LOCATION", Types.INTEGER, p -> p.getValue( 6 ) ),
            new MetaResultSetParameter<>( "INDEX_TYPE", Types.INTEGER, p -> p.getValue( 7 ) )
    );

    public static final List<MetaResultSetParameter<Procedure>> PROCEDURE_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "PROCEDURE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "PROCEDURE_SCHEM", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "PROCEDURE_NAME", Types.VARCHAR, Procedure::getTrivialName ),
            new MetaResultSetParameter<>( "reserved for future use", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "reserved for future use", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "reserved for future use", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, Procedure::getDescription ),
            new MetaResultSetParameter<>( "PROCEDURE_TYPE", Types.TINYINT, Procedure::getReturnTypeValue ),
            new MetaResultSetParameter<>( "SPECIFIC_NAME", Types.VARCHAR, Procedure::getUniqueName )
    );


    // Used to build an EMPTY result set thus no types and accessors are specified.
    public static final List<MetaResultSetParameter<ObjectUtils.Null>> PROCEDURE_COLUMN_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "PROCEDURE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "PROCEDURE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "PROCEDURE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "COLUMN_TYPE", Types.TINYINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "PRECISION", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "LENGTH", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SCALE", Types.TINYINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "RADIX", Types.TINYINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "NULLABLE", Types.TINYINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "COLUMN_DEF", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SQL_DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SQL_DATETIME_SUB", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "CHAR_OCTET_LENGTH", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ORDINAL_POSITION", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SPECIFIC_NAME", Types.VARCHAR, DUMMY_ACCESSOR )
    );

    public static final List<MetaResultSetParameter<GenericMetaContainer>> COLUMN_PRIVILEGES_GMC_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 0 ) ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, p -> p.getValue( 1 ) ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, p -> p.getValue( 2 ) ),
            new MetaResultSetParameter<>( "GRANTOR", Types.VARCHAR, p -> p.getValue( 3 ) ),
            new MetaResultSetParameter<>( "GRANTEE", Types.VARCHAR, p -> p.getValue( 4 ) ),
            new MetaResultSetParameter<>( "PRIVILEGE", Types.VARCHAR, p -> p.getValue( 5 ) ),
            new MetaResultSetParameter<>( "IS_GRANTABLE", Types.VARCHAR, p -> p.getValue( 6 ) )
    );

    public static final List<MetaResultSetParameter<GenericMetaContainer>> TABLE_PRIVILEGES_GMC_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> p.getValue( 0 ) ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, p -> p.getValue( 1 ) ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, p -> p.getValue( 2 ) ),
            new MetaResultSetParameter<>( "GRANTOR", Types.VARCHAR, p -> p.getValue( 3 ) ),
            new MetaResultSetParameter<>( "GRANTEE ", Types.VARCHAR, p -> p.getValue( 4 ) ),
            new MetaResultSetParameter<>( "PRIVILEGE", Types.VARCHAR, p -> p.getValue( 5 ) ),
            new MetaResultSetParameter<>( "IS_GRANTABLE", Types.VARCHAR, p -> p.getValue( 6 ) )
    );

    public static final List<MetaResultSetParameter<Column>> VERSION_COLUMN_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "SCOPE", Types.TINYINT, p -> null ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, Column::getColumnName ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName( p.getTypeName() ) ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, Column::getTypeName ),
            new MetaResultSetParameter<>( "COLUMN_SIZE", Types.INTEGER, Column::getTypeLength ),
            new MetaResultSetParameter<>( "BUFFER_LENGTH", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "DECIMAL_DIGITS", Types.TINYINT, nullIfFalse( convertScale( Column::getTypeScale ), Column::hasTypeScale ) ),
            new MetaResultSetParameter<>( "PSEUDO_COLUMN", Types.TINYINT, p -> p.getIsHidden()
                    ? DatabaseMetaData.versionColumnPseudo
                    : DatabaseMetaData.versionColumnNotPseudo )
    );

    // Used to build an EMPTY result set thus no types and accessors are specified.
    public static final List<MetaResultSetParameter<ObjectUtils.Null>> SUPER_TYPES_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SUPERTYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SUPERTYPE_SCHEM", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SUPERTYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR )
    );

    public static final List<MetaResultSetParameter<ObjectUtils.Null>> SUPER_TABLES_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SUPERTABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR )
    );

    public static final List<MetaResultSetParameter<ObjectUtils.Null>> ATTRIBUTES_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ATTR_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ATTR_TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ATTR_SIZE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "DECIMAL_DIGITS", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "NUM_PREC_RADIX", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ATTR_DEF", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SQL_DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SQL_DATETIME_SUB", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "CHAR_OCTET_LENGTH", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ORDINAL_POSITION", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SCOPE_CATALOG", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SCOPE_SCHEMA", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SCOPE_TABLE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SOURCE_DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR )
    );

    public static final List<MetaResultSetParameter<ClientInfoPropertyMeta>> CLIENT_INFO_PROPERTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "NAME", Types.VARCHAR, ClientInfoPropertyMeta::getKey ),
            new MetaResultSetParameter<>( "MAX_LEN", Types.VARCHAR, ClientInfoPropertyMeta::getMaxlength ),
            new MetaResultSetParameter<>( "DEFAULT_VALUE", Types.VARCHAR, ClientInfoPropertyMeta::getDefaultValue ),
            new MetaResultSetParameter<>( "DESCRIPTION", Types.VARCHAR, ClientInfoPropertyMeta::getDescription )
    );

    public static final List<MetaResultSetParameter<Column>> PSEUDO_COLUMN_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TABLE_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "TABLE_SCHEM", Types.VARCHAR, Column::getNamespaceName ),
            new MetaResultSetParameter<>( "TABLE_NAME", Types.VARCHAR, Column::getTableName ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, Column::getColumnName ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.VARCHAR, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName( p.getTypeName() ) ),
            new MetaResultSetParameter<>( "COLUMN_SIZE", Types.INTEGER, nullIfFalse( Column::getTypeLength, Column::hasTypeLength ) ),
            new MetaResultSetParameter<>( "DECIMAL_DIGITS", Types.INTEGER, nullIfFalse( convertScale( Column::getTypeScale ), Column::hasTypeScale ) ),
            new MetaResultSetParameter<>( "NUM_PREC_RADIX", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "COLUMN_USAGE", Types.VARCHAR, p -> PseudoColumnUsage.USAGE_UNKNOWN ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, p -> "" ),
            new MetaResultSetParameter<>( "CHAR_OCTET_LENGTH", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "IS_NULLABLE", Types.VARCHAR, p -> p.getIsNullable() ? "YES" : "NO" )
    );

    public static final List<MetaResultSetParameter<Column>> BEST_ROW_IDENTIFIER_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "SCOPE", Types.SMALLINT, integerAsShort( p -> DatabaseMetaData.bestRowSession ) ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, Column::getColumnName ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName( p.getTypeName() ) ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, Column::getTypeName ),
            new MetaResultSetParameter<>( "COLUMN_SIZE", Types.INTEGER, nullIfFalse( Column::getTypeLength, Column::hasTypeLength ) ),
            new MetaResultSetParameter<>( "BUFFER_LENGTH", Types.INTEGER, p -> null ),
            new MetaResultSetParameter<>( "DECIMAL_DIGITS", Types.SMALLINT, nullIfFalse( convertScale( Column::getTypeScale ), Column::hasTypeScale ) ),
            new MetaResultSetParameter<>( "PSEUDO_COLUMN", Types.SMALLINT, p -> p.getIsHidden()
                    ? DatabaseMetaData.bestRowPseudo
                    : DatabaseMetaData.bestRowNotPseudo
            )
    );

    // Used to build an EMPTY result set thus no types and accessors are specified.
    public static final List<MetaResultSetParameter<ObjectUtils.Null>> USER_DEFINED_TYPE_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "TYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "CLASS_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "BASE_TYPE", Types.SMALLINT, DUMMY_ACCESSOR )
    );

    // Used to build an EMPTY result set thus no types and accessors are specified.
    public static final List<MetaResultSetParameter<org.polypheny.db.protointerface.proto.Function>> FUNCTION_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "FUNCTION_CAT", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "FUNCTION_SCHEM", Types.VARCHAR, p -> null ),
            new MetaResultSetParameter<>( "FUNCTION_NAME", Types.VARCHAR, org.polypheny.db.protointerface.proto.Function::getName ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, org.polypheny.db.protointerface.proto.Function::getSyntax ),
            new MetaResultSetParameter<>( "FUNCTION_TYPE", Types.SMALLINT, p -> p.getIsTableFunction()
                    ? DatabaseMetaData.functionReturnsTable
                    : DatabaseMetaData.functionNoTable ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, org.polypheny.db.protointerface.proto.Function::getName )
    );

    // Used to build an EMPTY result set thus no types and accessors are specified.
    public static final List<MetaResultSetParameter<ObjectUtils.Null>> FUNCTION_COLUMN_EMPTY_SIGNATURE = Arrays.asList(
            new MetaResultSetParameter<>( "FUNCTION_CAT", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "FUNCTION_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "FUNCTION_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "COLUMN_NAME", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "COLUMN_TYPE", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "TYPE_NAME", Types.SMALLINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "PRECISION", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "LENGTH", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SCALE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "RADIX", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "NULLABLE", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "REMARKS", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "CHAR_OCTET_LENGTH", Types.SMALLINT, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "ORDINAL_POSITION", Types.INTEGER, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR ),
            new MetaResultSetParameter<>( "SPECIFIC_NAME", Types.SMALLINT, DUMMY_ACCESSOR )
    );


    private static <T> Function<T, Object> nullIfFalse( Function<T, Object> accessor, Function<T, Boolean> booleanFunction ) {
        return message -> {
            if ( booleanFunction.apply( message ) ) {
                return accessor.apply( message );
            }
            return null;
        };
    }


    private static <T> Function<T, Object> integerAsShort( Function<T, Object> accessor ) {
        return message -> {
            Object value = accessor.apply( message );
            if ( value instanceof Integer ) {
                return ((Integer) value).shortValue();
            }
            throw new IllegalArgumentException( "Can't convert this value to a short" );
        };
    }


    private static <T> Function<T, Object> convertScale( Function<T, Object> accessor ) {
        return message -> {
            Object value = accessor.apply( message );
            if ( !(value instanceof Integer) ) {
                throw new IllegalArgumentException( "Can't convert this value to a short" );
            }
            Integer integer = (Integer) value;
            if ( integer == -1 ) {
                return 0;
            }
            return integer.shortValue();
        };
    }

}
