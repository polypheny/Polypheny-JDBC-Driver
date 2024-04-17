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

package org.polypheny.jdbc.meta;

import java.sql.SQLException;
import java.util.function.Function;
import lombok.Getter;
import org.polypheny.jdbc.types.TypedValue;

class MetaResultSetParameter<T> {

    @Getter
    private final String name;
    @Getter
    private final int jdbcType;
    @Getter
    private final Function<T, Object> accessFunction;


    MetaResultSetParameter( String name, int jdbcType, Function<T, Object> acessor ) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.accessFunction = acessor;
    }


    TypedValue retrieveFrom( T message ) throws SQLException {
        return TypedValue.fromObject( accessFunction.apply( message ), jdbcType );
    }

}
