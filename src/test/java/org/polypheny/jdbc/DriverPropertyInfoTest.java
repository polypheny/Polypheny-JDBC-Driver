/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import org.junit.Test;

public class DriverPropertyInfoTest {

    @Test
    public void propertyInfo_defaultValues() throws SQLException {
        String url = "jdbc:polypheny://host:20590";
        DriverPropertyInfo[] infos = new PolyphenyDriver().getPropertyInfo( url, null );

    }

}
