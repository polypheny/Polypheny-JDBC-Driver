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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.Row;

public class PolyRow extends ArrayList<TypedValue> {

    private RelationalMetadata metadata;


    public PolyRow( List<TypedValue> value, RelationalMetadata metadata ) {
        super( value );
        this.metadata = metadata;
    }


    public TypedValue get( String columnName ) throws PrismInterfaceServiceException {
        try {
            int index = metadata.getColumnIndexFromLabel( columnName );
            return get( index );
        } catch ( SQLException e ) {
            throw new PrismInterfaceServiceException(
                    PrismInterfaceErrors.VALUE_ILLEGAL,
                    "Failed to retrieve column bsaed on the column name.",
                    e
            );
        }
    }


    public static PolyRow fromProto( Row protoRow, RelationalMetadata metadata ) {
        return new PolyRow( protoRow.getValuesList().stream().map( TypedValue::new ).collect( Collectors.toList() ), metadata );
    }

}
