package org.polypheny.jdbc.properties;

import lombok.Getter;
import org.polypheny.jdbc.ConnectionString;
import org.polypheny.jdbc.ProtoInterfaceClient;
import org.polypheny.jdbc.StatementProperties;
import org.polypheny.jdbc.proto.ConnectionProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class PolyphenyConnectionProperties {

    public PolyphenyConnectionProperties(ConnectionString connectionString, ProtoInterfaceClient protoInterfaceClient) throws SQLException {
        this.protoInterfaceClient = protoInterfaceClient;
        this.isAutoCommit = PropertyUtils.isDEFAULT_AUTOCOMMIT();
        this.isReadOnly = PropertyUtils.isDEFAULT_READ_ONLY();
        this.resultSetHoldability = PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY();
        this.networkTimeout = PropertyUtils.getDEFAULT_NETWORK_TIMEOUT();
        this.transactionIsolation = PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION();

        Map<String, String> parameters = connectionString.getParameters();
        Optional.ofNullable(parameters.get(PropertyUtils.getUSERNAME_KEY())).ifPresent(p -> this.username = p);
        Optional.ofNullable(parameters.get(PropertyUtils.getPASSWORD_KEY())).ifPresent(p -> this.password = p);
        Optional.ofNullable(parameters.get(PropertyUtils.getAUTOCOMMIT_KEY())).ifPresent(p -> this.isAutoCommit = Boolean.parseBoolean(p));
        Optional.ofNullable(parameters.get(PropertyUtils.getREAD_ONLY_KEY())).ifPresent(p -> this.isReadOnly = Boolean.parseBoolean(p));
        Optional.ofNullable(parameters.get(PropertyUtils.getNETWORK_TIMEOUT_KEY())).ifPresent(p -> this.networkTimeout = Integer.parseInt(p));
        Optional.ofNullable(parameters.get(PropertyUtils.getNAMESPACE_KEY())).ifPresent(p -> this.namespaceName = p);

        if (parameters.containsKey(PropertyUtils.getRESULT_SET_HOLDABILITY_KEY())) {
            int resultSetHoldability = parseResultSetHoldability(parameters.get(PropertyUtils.getRESULT_SET_HOLDABILITY_KEY()));
            if (!PropertyUtils.isValidResultSetHoldability(resultSetHoldability)) {
                throw new SQLException("Result set holdability not supported:" + resultSetHoldability);
            }
        }
        if (parameters.containsKey(PropertyUtils.getTRANSACTION_ISOLATION_KEY())) {
            int transactionIsolation = parseTransactionIsolation(parameters.get(PropertyUtils.getTRANSACTION_ISOLATION_KEY()));
            if (!PropertyUtils.isValidIsolationLevel(transactionIsolation)) {
                throw new SQLException("Transaction isolation level not supported: " + transactionIsolation);
            }
        }
    }

    private int parseTransactionIsolation(String string) throws SQLException {
        switch (string) {
            case "COMMITTED":
                return Connection.TRANSACTION_READ_COMMITTED;
            case "DIRTY":
                return Connection.TRANSACTION_READ_UNCOMMITTED;
            case "SERIALIZABLE":
                return Connection.TRANSACTION_SERIALIZABLE;
            case "REPEATABLE_READ":
                return Connection.TRANSACTION_REPEATABLE_READ;
        }
        throw new SQLException("Invalid value for transaction isolation: " + string);
    }

    private int parseResultSetHoldability(String string) throws SQLException {
        switch (string) {
            case "HOLD":
                return ResultSet.HOLD_CURSORS_OVER_COMMIT;
            case "CLOSE":
                return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        }
        throw new SQLException("Invalid value for result set holdability: " + string);
    }

    public ConnectionProperties.Holdability getProtoHoldability() {
        switch (resultSetHoldability) {
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                return ConnectionProperties.Holdability.CLOSE;
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                return ConnectionProperties.Holdability.HOLD;
        }
        throw new RuntimeException("Should never be thrown");
    }

    public ConnectionProperties.Isolation getProtoIsolation() {
        switch (transactionIsolation) {
            case Connection.TRANSACTION_READ_COMMITTED:
                return ConnectionProperties.Isolation.COMMITTED;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return ConnectionProperties.Isolation.DIRTY;
            case Connection.TRANSACTION_SERIALIZABLE:
                return ConnectionProperties.Isolation.SERIALIZABLE;
            case Connection.TRANSACTION_REPEATABLE_READ:
                    return ConnectionProperties.Isolation.REPEATABLE_READ;
        }
        throw new RuntimeException("Should never be thrown");
    }


    @Getter
    private ProtoInterfaceClient protoInterfaceClient;
    @Getter
    private String username;
    @Getter
    private String password;
    @Getter
    private boolean isAutoCommit;
    @Getter
    private boolean isReadOnly;
    @Getter
    private int resultSetHoldability;
    @Getter
    private int networkTimeout;
    @Getter
    private int transactionIsolation;
    @Getter
    // not transmitted to server
    private String catalogName;
    @Getter
    private String namespaceName;

    public void setAutoCommit(boolean isAutoCommit) {
        this.isAutoCommit = isAutoCommit;
        sync();
    }

    public void setReadOnly(boolean isReadOnly) {
        this.isReadOnly = isReadOnly;
        sync();
    }

    public void setResultSetHoldability(int resultSetHoldability) throws SQLException {
        if (!PropertyUtils.isValidResultSetHoldability(resultSetHoldability)) {
            throw new SQLException("Invalid value for result set holdability");
        }
        this.resultSetHoldability = resultSetHoldability;
        sync();
    }

    public void setNetworkTimeout(int networkTimeout) {
        this.networkTimeout = networkTimeout;
        sync();
    }

    public void setTransactionIsolation(int transactionIsolation) throws SQLException {
        if (!PropertyUtils.isValidIsolationLevel(transactionIsolation)) {
            throw new SQLException("Invalid value for transaction isolation");
        }
        this.transactionIsolation = transactionIsolation;
        sync();
    }

    public void setCatalogName(String catalogName) {
        // only there for consistency - no sync
        this.catalogName = catalogName;
    }

    public void setNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }


    private void sync() {
        protoInterfaceClient.setConnectionProperties(this);
    }


    public StatementProperties toStatementProperties() {
        return toStatementProperties(
                PropertyUtils.getDEFAULT_RESULTSET_TYPE(),
                PropertyUtils.getDEFAULT_RESULTSET_CONCURRENCY()
        );
    }


    public StatementProperties toStatementProperties(int resultSetType, int resultSetConcurrency) {
        return toStatementProperties(resultSetType, resultSetConcurrency, resultSetHoldability);
    }


    public StatementProperties toStatementProperties(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        StatementProperties properties = new StatementProperties();
        properties.setQueryTimeoutSeconds(PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS());
        properties.setResultSetType(resultSetType);
        properties.setResultSetConcurrency(resultSetConcurrency);
        properties.setResultSetHoldability(resultSetHoldability);
        properties.setFetchSize(PropertyUtils.getDEFAULT_FETCH_SIZE());
        properties.setFetchDirection(PropertyUtils.getDEFAULT_FETCH_DIRECTION());
        properties.setMaxFieldSize(PropertyUtils.getDEFAULT_MAX_FIELD_SIZE());
        properties.setLargeMaxRows(PropertyUtils.getDEFAULT_LARGE_MAX_ROWS());
        properties.setDoesEscapeProcessing(PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING());
        properties.setPoolable(PropertyUtils.isDEFAULT_STATEMENT_POOLABLE());
        return properties;
    }

}
