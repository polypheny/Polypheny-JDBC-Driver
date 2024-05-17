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

package org.polypheny.jdbc.multimodel;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.ColumnMeta;

public class RelationalMetadata {
    private List<RelationalColumnMetadata> columnMetas;
    private Map<String, Integer> columnIndexes;


    public RelationalMetadata( List<ColumnMeta> columnMetadata ) {
        this.columnMetas = columnMetadata.stream().map( RelationalColumnMetadata::new ).collect( Collectors.toList() );
        this.columnIndexes = this.columnMetas.stream().collect( Collectors.toMap( RelationalColumnMetadata::getColumnLabel, RelationalColumnMetadata::getColumnIndex, ( m, n ) -> n ) );

    }

    public RelationalColumnMetadata getColumnMeta( int columnIndex ) throws PrismInterfaceServiceException {
        try {
            return columnMetas.get( columnIndex );
        } catch ( IndexOutOfBoundsException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Column index out of bounds", e );
        }
    }


    public int getColumnIndexFromLabel( String columnLabel ) throws PrismInterfaceServiceException {
        Integer columnIndex = columnIndexes.get( columnLabel );
        if ( columnIndex == null ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.COLUMN_NOT_EXISTS, "Invalid column label: " + columnLabel );
        }
        return columnIndex;
    }


    public int getColumnCount() {
        return columnMetas.size();
    }

}
