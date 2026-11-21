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

package org.polypheny.jdbc.multimodel;

import lombok.Getter;
import org.polypheny.prism.ColumnMeta;

@Getter
public class RelationalColumnMetadata {

    private final int columnIndex;
    private final boolean isNullable;
    private final int length;
    private final String columnLabel;
    private final String columnName;
    private final int precision;

    private final String protocolTypeName;
    private final int scale;


    public RelationalColumnMetadata( ColumnMeta columnMeta ) {
        this.columnIndex = columnMeta.getColumnIndex();
        this.isNullable = columnMeta.getIsNullable();
        this.length = columnMeta.getLength();
        this.columnLabel = columnMeta.getColumnLabel();
        this.columnName = columnMeta.getColumnName();
        this.precision = columnMeta.getPrecision();
        this.protocolTypeName = columnMeta.getTypeMeta().getProtoValueType().name();
        this.scale = columnMeta.getScale();
    }

}
