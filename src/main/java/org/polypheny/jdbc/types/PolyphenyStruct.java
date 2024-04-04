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

package org.polypheny.jdbc.types;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PolyphenyStruct implements Struct {

    List<Object> attributes;
    String typeName;


    public PolyphenyStruct( String typeName, Object[] attributes ) {
        this.typeName = typeName;
        this.attributes = new ArrayList<>( Arrays.asList( attributes ) );
    }


    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }


    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes.toArray();
    }


    @Override
    public Object[] getAttributes( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }

}
