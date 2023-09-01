/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.jdbc.deserialization;

import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoRowId;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;
import org.polypheny.jdbc.PolyphenyRowId;

public class RowIdDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.ROWID:
                RowId rowId = deserializeToRowId( value.getRowId() );
                return TypedValue.fromObject( rowId, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto long." );
    }

    public RowId deserializeToRowId( ProtoRowId protoRowId ) {
        return new PolyphenyRowId(protoRowId.getRowId());
    }

}
