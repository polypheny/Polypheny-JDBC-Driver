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

import java.sql.ParameterMetaData;
import lombok.Getter;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class PolyphenyParameterMeta {

    /* As all values are unsigned in polypheny we hardcoded this. */
    private static final boolean SIGNEDNESS = false;
    private static final int PARAMETER_MODE = ParameterMetaData.parameterModeIn;
    private static final int NULLABILITY = ParameterMetaData.parameterNullableUnknown;

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
        this.parameterType = TypedValueUtils.getJdbcTypeFromPolyTypeName( parameterMeta.getTypeName() );
        this.parameterTypeName = parameterMeta.getTypeName();
        this.precision = parameterMeta.getPrecision();
        this.scale = parameterMeta.getScale();
        this.isNullable = NULLABILITY;
        this.isSigned = SIGNEDNESS;
    }

}
