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

package org.polypheny.jdbc.multimodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.db.protointerface.proto.Row;

public class PolyRow {

    List<PolyValue> values;


    public PolyRow( List<PolyValue> value ) {
        this.values = new ArrayList<>( value );
    }


    public PolyRow( PolyValue... value ) {
        this( Arrays.asList( value ) );
    }


    public int getColumnCount() {
        return values.size();
    }


    public PolyValue getValue( int columnIndex ) {
        return values.get( columnIndex );
    }


    public static <E extends PolyValue> PolyRow of( E... values ) {
        return new PolyRow( values );
    }


    public static PolyRow fromProto( Row protoRow ) {
        return new PolyRow( protoRow.getValuesList().stream().map( PolyValue::fromProto ).collect( Collectors.toList() ) );
    }

}
