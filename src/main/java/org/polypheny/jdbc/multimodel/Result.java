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

import lombok.Getter;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;

@Getter
public abstract class Result {

    private final ResultType resultType;


    public Result( ResultType resultType ) {
        this.resultType = resultType;
    }


    public <T> T unwrap( Class<T> aClass ) throws ProtoInterfaceServiceException {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


    public enum ResultType {
        RELATIONAL,
        DOCUMENT,
        GRAPH,
        SCALAR
    }


}
