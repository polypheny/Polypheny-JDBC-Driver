## 3.3

* Use Prism API 2.4.
* Expose server SQLSTATE and vendor error codes through `SQLException` while preserving the server message and existing exception type.


## 3.2

* Use Prism API 2.3.
* Support Cypher graph frames and typed nodes, edges, paths, and graphs in relational results.
* Preserve typed graph values in JDBC arrays and handle paging failures and request timeouts in multimodel statements.


## 3.1

* Use the released Prism API 2.2.
* Return document collection metadata from `PrismInterfaceClient.searchEntities()` when the server provides it.


## 3.0

### December 17, 2025

CHANGES:

* Use version 2.1 of PRISM
* Update dependencies
* Additional checks for result set type
* Improve closing of statements

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A


## 2.3

### December 28, 2024

CHANGES:

* Add null check for closing result sets
* Improve exception messages
* Handle null in asTimestamp
* Support converting BigDecimal to Long
* Treat column names as case-insensitive
* Return an empty List if parameterBatch is empty
* Use default timezone if calendar is null

IMPROVEMENTS:

* N/A

BUG FIXES:

* N/A

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
