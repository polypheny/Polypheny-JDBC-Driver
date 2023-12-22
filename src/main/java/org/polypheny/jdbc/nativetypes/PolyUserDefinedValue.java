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

package org.polypheny.jdbc.nativetypes;

import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class PolyUserDefinedValue extends PolyValue {

    @Getter
    private final Map<String, ProtoPolyType> template;
    @Getter
    private final Map<String, PolyValue> value;


    public PolyUserDefinedValue( Map<String, ProtoPolyType> template, Map<String, PolyValue> value ) {
        super( ProtoPolyType.USER_DEFINED_TYPE );
        this.template = template;
        this.value = value;
    }


    @Override
    public int compareTo( @NotNull PolyValue polyValue ) {
        if ( !isSameType( polyValue ) ) {
            return -1;
        }
        throw new NotImplementedException();
    }

}
