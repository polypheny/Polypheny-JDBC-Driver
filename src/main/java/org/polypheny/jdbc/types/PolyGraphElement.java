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

package org.polypheny.jdbc.types;


import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

@Getter
public class PolyGraphElement extends HashMap<String, TypedValue> {

    protected String id;
    protected String name;
    protected List<String> labels;


    public <T> T unwrap( Class<T> aClass ) throws PrismInterfaceServiceException {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


}
