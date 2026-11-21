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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.ProtoValue;

@Getter
public abstract class PolyGraphElement {

    private final String id;
    private final String name;
    private final List<String> labels;
    private final Map<String, TypedValue> properties;


    protected PolyGraphElement( String id, String name, List<String> labels, Map<String, ProtoValue> properties ) {
        this.id = id;
        this.name = name;
        this.labels = Collections.unmodifiableList( new ArrayList<>( labels ) );
        Map<String, TypedValue> values = new HashMap<>();
        properties.forEach( ( key, value ) -> values.put( key, new TypedValue( value ) ) );
        this.properties = Collections.unmodifiableMap( values );
    }


    public TypedValue get( String property ) {
        return properties.get( property );
    }


    public <T> T unwrap( Class<T> aClass ) throws PrismInterfaceServiceException {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


    public static PolyGraphElement of( GraphElement element ) {
        switch ( element.getElementCase() ) {
            case NODE:
                return new PolyNode( element.getNode() );
            case EDGE:
                return new PolyEdge( element.getEdge() );
            default:
                throw new IllegalArgumentException( "Unknown graph element of type " + element.getElementCase() );
        }
    }

}
