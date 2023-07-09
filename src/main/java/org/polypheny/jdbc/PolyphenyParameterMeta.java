package org.polypheny.jdbc;

import java.sql.ParameterMetaData;
import lombok.Getter;
import org.polypheny.jdbc.proto.ParameterMeta;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class PolyphenyParameterMeta {
    /* As all values are unsigned in polypheny we hardcoded this. */
    private static final boolean SIGNEDNESS = false;
    private static final int PARAMETER_MODE = ParameterMetaData.parameterModeIn;
    private static final int NULLABLITY = ParameterMetaData.parameterNullableUnknown;

    @Getter
    private String parameterClassName;
    @Getter
    private int parameterMode;
    @Getter
    private int parameterType;
    @Getter
    private String parameterTypeName;
    @Getter
    private int precision;
    @Getter
    private int scale;
    @Getter
    private int isNullable;
    @Getter
    private boolean isSigned;


    public PolyphenyParameterMeta( ParameterMeta parameterMeta ) {
        this.parameterClassName = null;
        this.parameterMode = PARAMETER_MODE;
        this.parameterType = TypedValueUtils.getJdbcTypeFromPolyTypeName( parameterMeta.getTypeName());
        this.parameterTypeName = parameterMeta.getTypeName();
        this.precision = parameterMeta.getPrecision();
        this.scale = parameterMeta.getScale();
        this.isNullable = NULLABLITY;
        this.isSigned = SIGNEDNESS;

    }
}
