/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.jdbc;


import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;


/**
 * Extension of {@link org.apache.calcite.avatica.AvaticaFactory} for Polypheny-DB.
 */
public abstract class PolyphenyJdbcFactory implements AvaticaFactory {

    private final int major;
    private final int minor;


    /**
     * Creates a JDBC factory with given major/minor version number.
     *
     * @param major JDBC major version
     * @param minor JDBC minor version
     */
    protected PolyphenyJdbcFactory( final int major, final int minor ) {
        this.major = major;
        this.minor = minor;
    }


    public int getJdbcMajorVersion() {
        return major;
    }


    public int getJdbcMinorVersion() {
        return minor;
    }


    public abstract AvaticaConnection newConnection( UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info );

}