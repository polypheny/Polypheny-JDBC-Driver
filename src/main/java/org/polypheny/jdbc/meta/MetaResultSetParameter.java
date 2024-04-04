package org.polypheny.jdbc.meta;

import lombok.Getter;
import org.polypheny.jdbc.types.TypedValue;

import java.sql.SQLException;
import java.util.function.Function;

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
