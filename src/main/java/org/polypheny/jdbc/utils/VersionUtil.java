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

package org.polypheny.jdbc.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VersionUtil {

    private static final String VERSION_FILE = "version.properties";
    private static final Properties properties = new Properties();


    static {
        try ( InputStream inputStream = VersionUtil.class.getClassLoader().getResourceAsStream( VERSION_FILE ) ) {
            properties.load( inputStream );
        } catch ( IOException e ) {
            log.error( "Error loading version.properties", e );
        }
    }


    public static String getVersion() {
        return properties.getProperty( "version" );
    }


    public static int getMajor() {
        return Integer.parseInt( properties.getProperty( "major" ) );
    }


    public static int getMinor() {
        return Integer.parseInt( properties.getProperty( "minor" ) );
    }


    public static String getQualifier() {
        return properties.getProperty( "qualifier" );
    }


    public static String getBuildTimestamp() {
        return properties.getProperty( "buildTimestamp" );
    }

}
