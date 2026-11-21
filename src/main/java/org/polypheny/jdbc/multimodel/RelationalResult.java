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

import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.Frame;

public class RelationalResult extends PagedResult<PolyRow> {

    @Getter
    private final RelationalMetadata metadata;


    public RelationalResult( Frame frame, PolyStatement polyStatement ) throws PrismInterfaceServiceException {
        this( frame, polyStatement, new RelationalMetadata( frame.getRelationalFrame().getColumnMetaList() ) );
    }


    private RelationalResult( Frame frame, PolyStatement polyStatement, RelationalMetadata metadata ) {
        super( ResultType.RELATIONAL, frame, polyStatement, page -> page.getRelationalFrame().getRowsList().stream()
                .map( row -> PolyRow.fromProto( row, metadata ) )
                .collect( Collectors.toList() ) );
        this.metadata = metadata;
    }

}
