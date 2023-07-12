package org.polypheny.jdbc.types;

public interface ExtraPolyTypes {

    // From JDK 1.6
    int ROWID = -8;
    int NCHAR = -15;
    int NVARCHAR = -9;
    int LONGNVARCHAR = -16;
    int NCLOB = 2011;
    int SQLXML = 2009;

    // From JDK 1.8
    int REF_CURSOR = 2012;
    int TIME_WITH_TIMEZONE = 2013;
    int TIMESTAMP_WITH_TIMEZONE = 2014;

    // From OpenGIS
    int GEOMETRY = 2015; // TODO: confirm
}