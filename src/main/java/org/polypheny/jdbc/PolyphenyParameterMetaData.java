package org.polypheny.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.utils.MetaUtils;

public class PolyphenyParameterMetaData implements ParameterMetaData {

    private int parameterCount;
    private List<PolyphenyParameterMeta> parameterMetas;


    public PolyphenyParameterMetaData( PreparedStatementSignature statementSignature ) {
        this.parameterCount = statementSignature.getParameterMetasCount();
        this.parameterMetas = MetaUtils.buildParameterMetas( statementSignature.getParameterMetasList() );
    }


    @Override
    public int getParameterCount() throws SQLException {
        return 0;
    }


    @Override
    public int isNullable( int i ) throws SQLException {
        return 0;
    }


    @Override
    public boolean isSigned( int i ) throws SQLException {
        return false;
    }


    @Override
    public int getPrecision( int i ) throws SQLException {
        return 0;
    }


    @Override
    public int getScale( int i ) throws SQLException {
        return 0;
    }


    @Override
    public int getParameterType( int i ) throws SQLException {
        return 0;
    }


    @Override
    public String getParameterTypeName( int i ) throws SQLException {
        return null;
    }


    @Override
    public String getParameterClassName( int i ) throws SQLException {
        return null;
    }


    @Override
    public int getParameterMode( int i ) throws SQLException {
        return 0;
    }


    @Override
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        return null;
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) throws SQLException {
        return false;
    }

}
