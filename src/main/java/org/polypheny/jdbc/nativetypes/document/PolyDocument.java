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

package org.polypheny.jdbc.nativetypes.document;

import java.util.HashMap;
import java.util.Map;
import org.polypheny.jdbc.nativetypes.PolyString;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;
import org.polypheny.db.protointerface.proto.ProtoDocument;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public class PolyDocument extends PolyMap<PolyString, PolyValue> {

    public PolyDocument( Map<PolyString, PolyValue> value ) {
        super( value, ProtoValueType.DOCUMENT );
    }


    public PolyDocument( PolyString key, PolyValue value ) {
        this( new HashMap<PolyString, PolyValue>() {{
            put( key, value );
        }} );
    }


    public static PolyDocument fromProto (ProtoDocument protoDocument ) {
        return PolyValue.deserializeToPolyDocument( protoDocument );
    }


    public static PolyDocument ofDocument( Map<PolyString, PolyValue> value ) {
        return new PolyDocument( value );
    }

}
