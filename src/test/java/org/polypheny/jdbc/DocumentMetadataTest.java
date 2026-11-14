/*
 * Copyright 2019-2026 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.utils.VersionUtil;
import org.polypheny.prism.Column;
import org.polypheny.prism.Document;
import org.polypheny.prism.DocumentField;
import org.polypheny.prism.DocumentIndex;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.Entity;
import org.polypheny.prism.Namespace;
import org.polypheny.prism.Response;
import org.polypheny.prism.Table;

class DocumentMetadataTest {

    private static final int TIMEOUT = 500;


    private static EntitiesResponse wireResponse( Entity... entities ) throws Exception {
        Response response = Response.newBuilder().setId( 7 ).setLast( true )
                .setEntitiesResponse( EntitiesResponse.newBuilder().addAllEntities( Arrays.asList( entities ) ) ).build();
        return Response.parseFrom( response.toByteArray() ).getEntitiesResponse();
    }


    private static Document document() {
        return Document.newBuilder().setNamespaceName( "patients" ).setCollectionName( "symptoms" )
                .addFields( DocumentField.newBuilder().setFieldPath( "patient_id" ).setTypeName( "string" ).setIsOptional( false ) )
                .addFields( DocumentField.newBuilder().setFieldPath( "complications.renal" ).setTypeName( "bool" ).setIsOptional( true ) )
                .setSampleDocumentJson( "{\"patient_id\":\"p1\",\"complications\":{\"renal\":true}}" )
                .addDocumentIndexes( DocumentIndex.newBuilder().setFieldPath( "patient_id" ).setIndexType( "btree" ).setUnique( true ) )
                .addDocumentIndexes( DocumentIndex.newBuilder().setFieldPath( "complications.$**" ).setIndexType( "wildcard" ).setUnique( false ) )
                .build();
    }


    @Test
    void existingSearchEntitiesReturnsEveryDocumentMetadataField() throws Exception {
        RpcService rpc = mock( RpcService.class );
        when( rpc.searchEntities( any( EntitiesRequest.class ), eq( TIMEOUT ) ) )
                .thenReturn( wireResponse( Entity.newBuilder().setDocument( document() ).build() ) );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );

        List<Entity> entities = client.searchEntities( "patients", "sym%", TIMEOUT );
        assertEquals( 1, entities.size() );
        assertTrue( entities.get( 0 ).hasDocument() );
        assertFalse( entities.get( 0 ).hasTable() );
        assertFalse( entities.get( 0 ).hasGraph() );
        Document metadata = entities.get( 0 ).getDocument();
        assertEquals( "patients", metadata.getNamespaceName() );
        assertEquals( "symptoms", metadata.getCollectionName() );
        assertEquals( 2, metadata.getFieldsCount() );
        assertEquals( "patient_id", metadata.getFields( 0 ).getFieldPath() );
        assertEquals( "string", metadata.getFields( 0 ).getTypeName() );
        assertFalse( metadata.getFields( 0 ).getIsOptional() );
        assertEquals( "complications.renal", metadata.getFields( 1 ).getFieldPath() );
        assertEquals( "bool", metadata.getFields( 1 ).getTypeName() );
        assertTrue( metadata.getFields( 1 ).getIsOptional() );
        assertTrue( metadata.hasSampleDocumentJson() );
        assertEquals( document().getSampleDocumentJson(), metadata.getSampleDocumentJson() );
        assertEquals( 2, metadata.getDocumentIndexesCount() );
        assertEquals( "patient_id", metadata.getDocumentIndexes( 0 ).getFieldPath() );
        assertEquals( "btree", metadata.getDocumentIndexes( 0 ).getIndexType() );
        assertTrue( metadata.getDocumentIndexes( 0 ).getUnique() );
        assertEquals( "complications.$**", metadata.getDocumentIndexes( 1 ).getFieldPath() );
        assertEquals( "wildcard", metadata.getDocumentIndexes( 1 ).getIndexType() );
        assertFalse( metadata.getDocumentIndexes( 1 ).getUnique() );

        ArgumentCaptor<EntitiesRequest> request = ArgumentCaptor.forClass( EntitiesRequest.class );
        verify( rpc ).searchEntities( request.capture(), eq( TIMEOUT ) );
        assertEquals( "patients", request.getValue().getNamespaceName() );
        assertTrue( request.getValue().hasEntityPattern() );
        assertEquals( "sym%", request.getValue().getEntityPattern() );
    }


    @Test
    void preservesAbsentAndExplicitlyEmptySamplesAndEmptyCollections() throws Exception {
        Document empty = Document.newBuilder().setNamespaceName( "patients" ).setCollectionName( "empty" ).build();
        RpcService rpc = mock( RpcService.class );
        when( rpc.searchEntities( any( EntitiesRequest.class ), eq( TIMEOUT ) ) )
                .thenReturn( wireResponse( Entity.newBuilder().setDocument( empty ).build() ) );

        Document metadata = new PrismInterfaceClient( rpc ).searchEntities( "patients", null, TIMEOUT ).get( 0 ).getDocument();
        assertFalse( metadata.hasSampleDocumentJson() );
        assertEquals( 0, metadata.getFieldsCount() );
        assertEquals( 0, metadata.getDocumentIndexesCount() );
        assertTrue( Document.parseFrom( empty.toBuilder().setSampleDocumentJson( "" ).build().toByteArray() ).hasSampleDocumentJson() );
        ArgumentCaptor<EntitiesRequest> request = ArgumentCaptor.forClass( EntitiesRequest.class );
        verify( rpc ).searchEntities( request.capture(), eq( TIMEOUT ) );
        assertFalse( request.getValue().hasEntityPattern() );
    }


    @Test
    void preservesLegacyEmptyDocumentMessagesAndEscapedFieldPaths() throws Exception {
        assertEquals( Document.getDefaultInstance(), Document.parseFrom( new byte[0] ) );
        DocumentField field = DocumentField.newBuilder().setFieldPath( "literal\\.dot.slash\\\\key" ).setTypeName( "mixed" ).setIsOptional( true ).build();
        assertEquals( field, DocumentField.parseFrom( field.toByteArray() ) );
        assertEquals( "literal\\.dot.slash\\\\key", DocumentField.parseFrom( field.toByteArray() ).getFieldPath() );
    }


    @Test
    void usesPrism22NegotiationAndStableDocumentFieldNumbers() {
        assertEquals( 2, VersionUtil.MAJOR_API_VERSION );
        assertEquals( 2, VersionUtil.MINOR_API_VERSION );
        assertEquals( 1, Document.NAMESPACE_NAME_FIELD_NUMBER );
        assertEquals( 2, Document.COLLECTION_NAME_FIELD_NUMBER );
        assertEquals( 3, Document.FIELDS_FIELD_NUMBER );
        assertEquals( 4, Document.SAMPLE_DOCUMENT_JSON_FIELD_NUMBER );
        assertEquals( 5, Document.DOCUMENT_INDEXES_FIELD_NUMBER );
        assertEquals( 1, DocumentField.FIELD_PATH_FIELD_NUMBER );
        assertEquals( 2, DocumentField.TYPE_NAME_FIELD_NUMBER );
        assertEquals( 3, DocumentField.IS_OPTIONAL_FIELD_NUMBER );
        assertEquals( 1, DocumentIndex.FIELD_PATH_FIELD_NUMBER );
        assertEquals( 2, DocumentIndex.INDEX_TYPE_FIELD_NUMBER );
        assertEquals( 3, DocumentIndex.UNIQUE_FIELD_NUMBER );
    }


    @Test
    void jdbcTableAndColumnMetadataStillSelectsOnlyRelationalEntities() throws Exception {
        RpcService rpc = mock( RpcService.class );
        Table table = Table.newBuilder().setNamespaceName( "patients" ).setTableName( "patients_info" ).setTableType( "ENTITY" )
                .addColumns( Column.newBuilder().setNamespaceName( "patients" ).setTableName( "patients_info" )
                        .setColumnName( "patient_id" ).setTypeName( "VARCHAR" ).setColumnIndex( 1 ) ).build();
        when( rpc.searchEntities( any( EntitiesRequest.class ), eq( TIMEOUT ) ) )
                .thenReturn( wireResponse( Entity.newBuilder().setDocument( document() ).build(), Entity.newBuilder().setTable( table ).build() ) );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc ) {
            @Override
            public List<Namespace> searchNamespaces( String pattern, String type, int timeout ) {
                assertEquals( "RELATIONAL", type );
                return Collections.singletonList( Namespace.newBuilder().setNamespaceName( "patients" ).setNamespaceType( "RELATIONAL" ).build() );
            }
        };
        PolyphenyDatabaseMetadata metadata = new PolyphenyDatabaseMetadata( client, new ConnectionString( "jdbc:polypheny://localhost:20590" ) );
        PolyConnection connection = mock( PolyConnection.class );
        when( connection.getNetworkTimeout() ).thenReturn( TIMEOUT );
        metadata.setConnection( connection );

        try ( ResultSet tables = metadata.getTables( null, "patients", "%", null ) ) {
            assertTrue( tables.next() );
            assertEquals( "patients_info", tables.getString( "TABLE_NAME" ) );
            assertFalse( tables.next() );
        }
        try ( ResultSet columns = metadata.getColumns( null, "patients", "%", "%" ) ) {
            assertTrue( columns.next() );
            assertEquals( "patient_id", columns.getString( "COLUMN_NAME" ) );
            assertEquals( "VARCHAR", columns.getString( "TYPE_NAME" ) );
            assertFalse( columns.next() );
        }
    }


    @Test
    void propagatesServerErrorsThroughTheExistingApi() throws Exception {
        RpcService rpc = mock( RpcService.class );
        when( rpc.searchEntities( any( EntitiesRequest.class ), eq( TIMEOUT ) ) ).thenThrow( new PrismInterfaceServiceException( "metadata unavailable" ) );
        assertThrows( PrismInterfaceServiceException.class, () -> new PrismInterfaceClient( rpc ).searchEntities( "patients", null, TIMEOUT ) );
    }

}
