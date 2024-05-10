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

package org.polypheny.jdbc.properties;

import java.sql.SQLException;
import java.util.Calendar;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.PolyphenyStatement;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class PolyphenyStatementProperties {

    private static final int UNSET_INT = -1;

    @Setter
    PrismInterfaceClient prismInterfaceClient;
    PolyphenyStatement polyphenyStatement;
    @Getter
    private int queryTimeoutSeconds;
    @Getter
    private int resultSetType = UNSET_INT;
    @Getter
    private int resultSetConcurrency = UNSET_INT;
    @Getter
    private int resultSetHoldability = UNSET_INT;
    @Getter
    private int fetchSize;
    @Getter
    private int fetchDirection;
    @Getter
    private int maxFieldSize;
    @Getter
    private long largeMaxRows;
    @Getter
    private boolean doesEscapeProcessing;
    @Getter
    private boolean isPoolable;
    @Getter
    @Setter
    private Calendar calendar;
    @Getter
    @Setter
    private boolean isCloseOnCompletion;


    public void setPolyphenyStatement( PolyphenyStatement polyphenyStatement ) throws SQLException {
        if ( this.polyphenyStatement != null ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Can't change polyphenyStatement" + polyphenyStatement );
        }
        this.polyphenyStatement = polyphenyStatement;
    }


    public void setQueryTimeoutSeconds( int queryTimeoutSeconds ) throws SQLException {
        if ( queryTimeoutSeconds < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for max" );
        }
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }


    public void setResultSetType( int resultSetType ) throws SQLException {
        if ( this.resultSetType != UNSET_INT ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Can't change result set type" );
        }
        if ( !PropertyUtils.isValidResultSetType( resultSetType ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set type" );
        }
        this.resultSetType = resultSetType;
    }


    public void setResultSetConcurrency( int resultSetConcurrency ) throws SQLException {
        if ( this.resultSetConcurrency != UNSET_INT ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Can't change result set type" );
        }
        if ( !PropertyUtils.isValidResultSetConcurrency( resultSetConcurrency ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set concurrency" );
        }
        this.resultSetConcurrency = resultSetConcurrency;
    }


    public void setResultSetHoldability( int resultSetHoldability ) throws SQLException {
        if ( this.resultSetHoldability != UNSET_INT ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Can't change result set type" );
        }
        if ( !PropertyUtils.isValidResultSetHoldability( resultSetHoldability ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set concurrency" );
        }
        this.resultSetHoldability = resultSetHoldability;
        // not transmitted to server -> no sync()
    }


    public void setFetchSize( int fetchSize ) throws SQLException {
        if ( fetchSize < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for fetch size" );
        }
        this.fetchSize = fetchSize;
    }


    public void setFetchDirection( int fetchDirection ) throws SQLException {
        if ( PropertyUtils.isInvalidFetchDirection( fetchDirection ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for fetch direction" );
        }
        this.fetchDirection = fetchDirection;
    }


    public void setMaxFieldSize( int maxFieldSize ) throws SQLException {
        if ( maxFieldSize < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Illegal argument for max field size" );
        }
        this.maxFieldSize = maxFieldSize;
    }


    public void setLargeMaxRows( long largeMaxRows ) throws SQLException {
        if ( largeMaxRows < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for large max rows" );
        }
        this.largeMaxRows = largeMaxRows;
    }


    public void setDoesEscapeProcessing( boolean doesEscapeProcessing ) throws SQLException {
        this.doesEscapeProcessing = doesEscapeProcessing;
    }


    public void setIsPoolable( boolean isPoolable ) throws SQLException {
        this.isPoolable = isPoolable;
    }


    public PolyphenyResultSetProperties toResultSetProperties() {
        PolyphenyResultSetProperties properties = new PolyphenyResultSetProperties();
        properties.setResultSetType( resultSetType );
        properties.setResultSetConcurrency( resultSetConcurrency );
        properties.setResultSetHoldability( resultSetHoldability );
        properties.setFetchDirection( fetchDirection );
        properties.setStatementFetchSize( fetchSize );
        properties.setMaxFieldSize( maxFieldSize );
        properties.setLargeMaxRows( largeMaxRows );
        properties.setCalendar( calendar );
        return properties;
    }

}
