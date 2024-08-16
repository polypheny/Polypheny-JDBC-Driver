## 2.2

### August 16, 2024

CHANGES:

* Improved toString method to align with the standard-compliant asString implementation.
* Updated fromDate method to accept null as input when using a custom calendar, representing a missing value.

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A

## 2.1

### June 12, 2024

CHANGES:

* Improve handling of connection resets on Windows.

IMPROVEMENTS:

* N/A

BUG FIXES:

* When a DisconnectRequest is sent, the server sends a DisconnectResponse and closes the connection. On Windows it could happen that the closing of the connection is reported via exception before the DisconnectResponse has been processed.

## 2.0

### May 10, 2024

CHANGES:

* New version of JDBC driver utilizing the Polypheny Prism query interface and protocol.

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A

## 1.5.3

### November 21, 2021

CHANGES:

* Upgrade SLF4J to version 2.0.3
* Upgrade Apache Commons Lang3 to version 3.12.0

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A

## 1.5.2

### November 21, 2021

CHANGES:

* Introduced version 1.17.2 of Avatica for Polypheny.

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A
