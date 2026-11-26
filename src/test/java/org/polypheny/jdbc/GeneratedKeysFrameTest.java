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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;


class GeneratedKeysFrameTest {

    @ParameterizedTest
    @ValueSource(ints = { 0, 2 })
    void rejectsMalformedDirectResultRows( int badRowWidth ) {
        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> GeneratedKeys.requireComplete( keyFrame( badRowWidth ) ) );
        assertEquals( PrismInterfaceErrors.RESULT_TYPE_INVALID.state, failure.getSQLState() );
    }


    @ParameterizedTest
    @CsvSource({ "1, 0", "1, 2", "2, 0", "2, 2" })
    void rejectsMalformedBatchResultRows( int batchSize, int badRowWidth ) {
        List<BatchGeneratedKeys> batch = new ArrayList<>();
        if ( batchSize == 2 ) {
            batch.add( BatchGeneratedKeys.newBuilder().setStatementIndex( 0 ).setKeys( keyFrame( 1 ) ).build() );
        }
        batch.add( BatchGeneratedKeys.newBuilder().setStatementIndex( batchSize - 1 ).setKeys( keyFrame( badRowWidth ) ).build() );

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, () -> GeneratedKeys.combine( batch ) );
        assertEquals( PrismInterfaceErrors.RESULT_TYPE_INVALID.state, failure.getSQLState() );
    }


    private static Frame keyFrame( int secondRowWidth ) {
        Row.Builder row = Row.newBuilder();
        for ( int i = 0; i < secondRowWidth; i++ ) {
            row.addValues( ProtoValue.getDefaultInstance() );
        }
        return Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.newBuilder()
                .addColumnMeta( ColumnMeta.getDefaultInstance() )
                .addRows( Row.newBuilder().addValues( ProtoValue.getDefaultInstance() ) )
                .addRows( row ) ).build();
    }

}
