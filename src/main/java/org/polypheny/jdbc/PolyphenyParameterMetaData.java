package org.polypheny.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import lombok.Getter;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.utils.MetaUtils;

public class PolyphenyParameterMetaData implements ParameterMetaData {

    private int parameterCount;
    private List<PolyphenyParameterMeta> parameterMetas;


    public PolyphenyParameterMetaData( PreparedStatementSignature statementSignature ) {
        this.parameterCount = statementSignature.getParameterMetasCount();
        this.parameterMetas = MetaUtils.buildParameterMetas( statementSignature.getParameterMetasList() );
    }


    private void throwIfOutOfBounds( int param ) throws SQLException {
        /* jdbc indexes start with 1 */
        param--;
        if ( param < 0 ) {
            throw new SQLException( "Index out of Bounds." );
        }
        if ( param >= parameterCount ) {
            throw new SQLException( "Index out of Bounds." );
        }
    }


    private PolyphenyParameterMeta getMeta( int param ) throws SQLException {
        throwIfOutOfBounds( param );
        return parameterMetas.get( param );
    }


    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }


    @Override
    public int isNullable( int param ) throws SQLException {
        return getMeta( param ).getIsNullable();
    }


    @Override
    public boolean isSigned( int param ) throws SQLException {
        return getMeta( param ).isSigned();
    }


    @Override
    public int getPrecision( int param ) throws SQLException {
        return getMeta( param ).getPrecision();
    }


    @Override
    public int getScale( int param ) throws SQLException {
        return getMeta( param ).getScale();
    }


    @Override
    public int getParameterType( int param ) throws SQLException {
        return getMeta( param ).getParameterType();
    }


    @Override
    public String getParameterTypeName( int param ) throws SQLException {
        return getMeta( param ).getParameterTypeName();
    }


    @Override
    public String getParameterClassName( int param ) throws SQLException {
        return getMeta( param ).getParameterClassName();
    }


    @Override
    public int getParameterMode( int param ) throws SQLException {
        return getMeta( param ).getParameterMode();
    }


    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new SQLException("Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return aClass.isInstance(this);

    }

}
