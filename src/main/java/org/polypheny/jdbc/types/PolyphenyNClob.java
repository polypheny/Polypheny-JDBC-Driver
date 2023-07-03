package org.polypheny.jdbc.types;

import java.sql.NClob;

public class PolyphenyNClob extends PolyphenyClob implements NClob {
    /* As java strings are used as the dbms native representation, there is no difference to PolyphenyClob */
}
