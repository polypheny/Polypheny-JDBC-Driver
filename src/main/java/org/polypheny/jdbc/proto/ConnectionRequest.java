package org.polypheny.jdbc.proto;// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Polypheny-JDBC-Driver/src/main/proto/protointerface.proto

/**
 * <pre>
 * Sent by the client to request a connection to the server.
 * </pre>
 *
 * Protobuf type {@code org.polypheny.jdbc.ConnectionRequest}
 */
public  final class ConnectionRequest extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:org.polypheny.jdbc.ConnectionRequest)
        ConnectionRequestOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ConnectionRequest.newBuilder() to construct.
  private ConnectionRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ConnectionRequest() {
    majorApiVersion_ = 0;
    minorApiVersion_ = 0;
    clientUUID_ = "";
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ConnectionRequest(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          case 8: {

            majorApiVersion_ = input.readUInt32();
            break;
          }
          case 16: {

            minorApiVersion_ = input.readUInt32();
            break;
          }
          case 26: {
            String s = input.readStringRequireUtf8();

            clientUUID_ = s;
            break;
          }
          case 34: {
            if (!((mutable_bitField0_ & 0x00000008) == 0x00000008)) {
              connectionProperties_ = com.google.protobuf.MapField.newMapField(
                  ConnectionPropertiesDefaultEntryHolder.defaultEntry);
              mutable_bitField0_ |= 0x00000008;
            }
            com.google.protobuf.MapEntry<String, String>
            connectionProperties__ = input.readMessage(
                ConnectionPropertiesDefaultEntryHolder.defaultEntry.getParserForType(), extensionRegistry);
            connectionProperties_.getMutableMap().put(
                connectionProperties__.getKey(), connectionProperties__.getValue());
            break;
          }
          default: {
            if (!parseUnknownFieldProto3(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_descriptor;
  }

  @SuppressWarnings({"rawtypes"})
  @Override
  protected com.google.protobuf.MapField internalGetMapField(
      int number) {
    switch (number) {
      case 4:
        return internalGetConnectionProperties();
      default:
        throw new RuntimeException(
            "Invalid map field number: " + number);
    }
  }
  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            ConnectionRequest.class, Builder.class);
  }

  private int bitField0_;
  public static final int MAJORAPIVERSION_FIELD_NUMBER = 1;
  private int majorApiVersion_;
  /**
   * <code>uint32 majorApiVersion = 1;</code>
   */
  public int getMajorApiVersion() {
    return majorApiVersion_;
  }

  public static final int MINORAPIVERSION_FIELD_NUMBER = 2;
  private int minorApiVersion_;
  /**
   * <code>uint32 minorApiVersion = 2;</code>
   */
  public int getMinorApiVersion() {
    return minorApiVersion_;
  }

  public static final int CLIENTUUID_FIELD_NUMBER = 3;
  private volatile Object clientUUID_;
  /**
   * <pre>
   * UUID generated by the client. Enables the server to identify clients.
   * </pre>
   *
   * <code>string clientUUID = 3;</code>
   */
  public String getClientUUID() {
    Object ref = clientUUID_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      clientUUID_ = s;
      return s;
    }
  }
  /**
   * <pre>
   * UUID generated by the client. Enables the server to identify clients.
   * </pre>
   *
   * <code>string clientUUID = 3;</code>
   */
  public com.google.protobuf.ByteString
      getClientUUIDBytes() {
    Object ref = clientUUID_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      clientUUID_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int CONNECTIONPROPERTIES_FIELD_NUMBER = 4;
  private static final class ConnectionPropertiesDefaultEntryHolder {
    static final com.google.protobuf.MapEntry<
        String, String> defaultEntry =
            com.google.protobuf.MapEntry
            .<String, String>newDefaultInstance(
                ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_ConnectionPropertiesEntry_descriptor, 
                com.google.protobuf.WireFormat.FieldType.STRING,
                "",
                com.google.protobuf.WireFormat.FieldType.STRING,
                "");
  }
  private com.google.protobuf.MapField<
      String, String> connectionProperties_;
  private com.google.protobuf.MapField<String, String>
  internalGetConnectionProperties() {
    if (connectionProperties_ == null) {
      return com.google.protobuf.MapField.emptyMapField(
          ConnectionPropertiesDefaultEntryHolder.defaultEntry);
    }
    return connectionProperties_;
  }

  public int getConnectionPropertiesCount() {
    return internalGetConnectionProperties().getMap().size();
  }
  /**
   * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
   */

  public boolean containsConnectionProperties(
      String key) {
    if (key == null) { throw new NullPointerException(); }
    return internalGetConnectionProperties().getMap().containsKey(key);
  }
  /**
   * Use {@link #getConnectionPropertiesMap()} instead.
   */
  @Deprecated
  public java.util.Map<String, String> getConnectionProperties() {
    return getConnectionPropertiesMap();
  }
  /**
   * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
   */

  public java.util.Map<String, String> getConnectionPropertiesMap() {
    return internalGetConnectionProperties().getMap();
  }
  /**
   * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
   */

  public String getConnectionPropertiesOrDefault(
      String key,
      String defaultValue) {
    if (key == null) { throw new NullPointerException(); }
    java.util.Map<String, String> map =
        internalGetConnectionProperties().getMap();
    return map.containsKey(key) ? map.get(key) : defaultValue;
  }
  /**
   * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
   */

  public String getConnectionPropertiesOrThrow(
      String key) {
    if (key == null) { throw new NullPointerException(); }
    java.util.Map<String, String> map =
        internalGetConnectionProperties().getMap();
    if (!map.containsKey(key)) {
      throw new IllegalArgumentException();
    }
    return map.get(key);
  }

  private byte memoizedIsInitialized = -1;
  @Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (majorApiVersion_ != 0) {
      output.writeUInt32(1, majorApiVersion_);
    }
    if (minorApiVersion_ != 0) {
      output.writeUInt32(2, minorApiVersion_);
    }
    if (!getClientUUIDBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, clientUUID_);
    }
    com.google.protobuf.GeneratedMessageV3
      .serializeStringMapTo(
        output,
        internalGetConnectionProperties(),
        ConnectionPropertiesDefaultEntryHolder.defaultEntry,
        4);
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (majorApiVersion_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeUInt32Size(1, majorApiVersion_);
    }
    if (minorApiVersion_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeUInt32Size(2, minorApiVersion_);
    }
    if (!getClientUUIDBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, clientUUID_);
    }
    for (java.util.Map.Entry<String, String> entry
         : internalGetConnectionProperties().getMap().entrySet()) {
      com.google.protobuf.MapEntry<String, String>
      connectionProperties__ = ConnectionPropertiesDefaultEntryHolder.defaultEntry.newBuilderForType()
          .setKey(entry.getKey())
          .setValue(entry.getValue())
          .build();
      size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(4, connectionProperties__);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof ConnectionRequest)) {
      return super.equals(obj);
    }
    ConnectionRequest other = (ConnectionRequest) obj;

    boolean result = true;
    result = result && (getMajorApiVersion()
        == other.getMajorApiVersion());
    result = result && (getMinorApiVersion()
        == other.getMinorApiVersion());
    result = result && getClientUUID()
        .equals(other.getClientUUID());
    result = result && internalGetConnectionProperties().equals(
        other.internalGetConnectionProperties());
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + MAJORAPIVERSION_FIELD_NUMBER;
    hash = (53 * hash) + getMajorApiVersion();
    hash = (37 * hash) + MINORAPIVERSION_FIELD_NUMBER;
    hash = (53 * hash) + getMinorApiVersion();
    hash = (37 * hash) + CLIENTUUID_FIELD_NUMBER;
    hash = (53 * hash) + getClientUUID().hashCode();
    if (!internalGetConnectionProperties().getMap().isEmpty()) {
      hash = (37 * hash) + CONNECTIONPROPERTIES_FIELD_NUMBER;
      hash = (53 * hash) + internalGetConnectionProperties().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static ConnectionRequest parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionRequest parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionRequest parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionRequest parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionRequest parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionRequest parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionRequest parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ConnectionRequest parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static ConnectionRequest parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static ConnectionRequest parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static ConnectionRequest parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ConnectionRequest parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(ConnectionRequest prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @Override
  protected Builder newBuilderForType(
      BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * <pre>
   * Sent by the client to request a connection to the server.
   * </pre>
   *
   * Protobuf type {@code org.polypheny.jdbc.ConnectionRequest}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:org.polypheny.jdbc.ConnectionRequest)
          ConnectionRequestOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_descriptor;
    }

    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMapField(
        int number) {
      switch (number) {
        case 4:
          return internalGetConnectionProperties();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @SuppressWarnings({"rawtypes"})
    protected com.google.protobuf.MapField internalGetMutableMapField(
        int number) {
      switch (number) {
        case 4:
          return internalGetMutableConnectionProperties();
        default:
          throw new RuntimeException(
              "Invalid map field number: " + number);
      }
    }
    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              ConnectionRequest.class, Builder.class);
    }

    // Construct using ConnectionRequest.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    @Override
    public Builder clear() {
      super.clear();
      majorApiVersion_ = 0;

      minorApiVersion_ = 0;

      clientUUID_ = "";

      internalGetMutableConnectionProperties().clear();
      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionRequest_descriptor;
    }

    @Override
    public ConnectionRequest getDefaultInstanceForType() {
      return ConnectionRequest.getDefaultInstance();
    }

    @Override
    public ConnectionRequest build() {
      ConnectionRequest result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public ConnectionRequest buildPartial() {
      ConnectionRequest result = new ConnectionRequest(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      result.majorApiVersion_ = majorApiVersion_;
      result.minorApiVersion_ = minorApiVersion_;
      result.clientUUID_ = clientUUID_;
      result.connectionProperties_ = internalGetConnectionProperties();
      result.connectionProperties_.makeImmutable();
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    @Override
    public Builder clone() {
      return (Builder) super.clone();
    }
    @Override
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    @Override
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    @Override
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    @Override
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    @Override
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof ConnectionRequest) {
        return mergeFrom((ConnectionRequest)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(ConnectionRequest other) {
      if (other == ConnectionRequest.getDefaultInstance()) return this;
      if (other.getMajorApiVersion() != 0) {
        setMajorApiVersion(other.getMajorApiVersion());
      }
      if (other.getMinorApiVersion() != 0) {
        setMinorApiVersion(other.getMinorApiVersion());
      }
      if (!other.getClientUUID().isEmpty()) {
        clientUUID_ = other.clientUUID_;
        onChanged();
      }
      internalGetMutableConnectionProperties().mergeFrom(
          other.internalGetConnectionProperties());
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    @Override
    public final boolean isInitialized() {
      return true;
    }

    @Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      ConnectionRequest parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (ConnectionRequest) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private int majorApiVersion_ ;
    /**
     * <code>uint32 majorApiVersion = 1;</code>
     */
    public int getMajorApiVersion() {
      return majorApiVersion_;
    }
    /**
     * <code>uint32 majorApiVersion = 1;</code>
     */
    public Builder setMajorApiVersion(int value) {
      
      majorApiVersion_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>uint32 majorApiVersion = 1;</code>
     */
    public Builder clearMajorApiVersion() {
      
      majorApiVersion_ = 0;
      onChanged();
      return this;
    }

    private int minorApiVersion_ ;
    /**
     * <code>uint32 minorApiVersion = 2;</code>
     */
    public int getMinorApiVersion() {
      return minorApiVersion_;
    }
    /**
     * <code>uint32 minorApiVersion = 2;</code>
     */
    public Builder setMinorApiVersion(int value) {
      
      minorApiVersion_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>uint32 minorApiVersion = 2;</code>
     */
    public Builder clearMinorApiVersion() {
      
      minorApiVersion_ = 0;
      onChanged();
      return this;
    }

    private Object clientUUID_ = "";
    /**
     * <pre>
     * UUID generated by the client. Enables the server to identify clients.
     * </pre>
     *
     * <code>string clientUUID = 3;</code>
     */
    public String getClientUUID() {
      Object ref = clientUUID_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        clientUUID_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <pre>
     * UUID generated by the client. Enables the server to identify clients.
     * </pre>
     *
     * <code>string clientUUID = 3;</code>
     */
    public com.google.protobuf.ByteString
        getClientUUIDBytes() {
      Object ref = clientUUID_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        clientUUID_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * UUID generated by the client. Enables the server to identify clients.
     * </pre>
     *
     * <code>string clientUUID = 3;</code>
     */
    public Builder setClientUUID(
        String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      clientUUID_ = value;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * UUID generated by the client. Enables the server to identify clients.
     * </pre>
     *
     * <code>string clientUUID = 3;</code>
     */
    public Builder clearClientUUID() {
      
      clientUUID_ = getDefaultInstance().getClientUUID();
      onChanged();
      return this;
    }
    /**
     * <pre>
     * UUID generated by the client. Enables the server to identify clients.
     * </pre>
     *
     * <code>string clientUUID = 3;</code>
     */
    public Builder setClientUUIDBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      clientUUID_ = value;
      onChanged();
      return this;
    }

    private com.google.protobuf.MapField<
        String, String> connectionProperties_;
    private com.google.protobuf.MapField<String, String>
    internalGetConnectionProperties() {
      if (connectionProperties_ == null) {
        return com.google.protobuf.MapField.emptyMapField(
            ConnectionPropertiesDefaultEntryHolder.defaultEntry);
      }
      return connectionProperties_;
    }
    private com.google.protobuf.MapField<String, String>
    internalGetMutableConnectionProperties() {
      onChanged();;
      if (connectionProperties_ == null) {
        connectionProperties_ = com.google.protobuf.MapField.newMapField(
            ConnectionPropertiesDefaultEntryHolder.defaultEntry);
      }
      if (!connectionProperties_.isMutable()) {
        connectionProperties_ = connectionProperties_.copy();
      }
      return connectionProperties_;
    }

    public int getConnectionPropertiesCount() {
      return internalGetConnectionProperties().getMap().size();
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public boolean containsConnectionProperties(
        String key) {
      if (key == null) { throw new NullPointerException(); }
      return internalGetConnectionProperties().getMap().containsKey(key);
    }
    /**
     * Use {@link #getConnectionPropertiesMap()} instead.
     */
    @Deprecated
    public java.util.Map<String, String> getConnectionProperties() {
      return getConnectionPropertiesMap();
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public java.util.Map<String, String> getConnectionPropertiesMap() {
      return internalGetConnectionProperties().getMap();
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public String getConnectionPropertiesOrDefault(
        String key,
        String defaultValue) {
      if (key == null) { throw new NullPointerException(); }
      java.util.Map<String, String> map =
          internalGetConnectionProperties().getMap();
      return map.containsKey(key) ? map.get(key) : defaultValue;
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public String getConnectionPropertiesOrThrow(
        String key) {
      if (key == null) { throw new NullPointerException(); }
      java.util.Map<String, String> map =
          internalGetConnectionProperties().getMap();
      if (!map.containsKey(key)) {
        throw new IllegalArgumentException();
      }
      return map.get(key);
    }

    public Builder clearConnectionProperties() {
      internalGetMutableConnectionProperties().getMutableMap()
          .clear();
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public Builder removeConnectionProperties(
        String key) {
      if (key == null) { throw new NullPointerException(); }
      internalGetMutableConnectionProperties().getMutableMap()
          .remove(key);
      return this;
    }
    /**
     * Use alternate mutation accessors instead.
     */
    @Deprecated
    public java.util.Map<String, String>
    getMutableConnectionProperties() {
      return internalGetMutableConnectionProperties().getMutableMap();
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */
    public Builder putConnectionProperties(
        String key,
        String value) {
      if (key == null) { throw new NullPointerException(); }
      if (value == null) { throw new NullPointerException(); }
      internalGetMutableConnectionProperties().getMutableMap()
          .put(key, value);
      return this;
    }
    /**
     * <code>map&lt;string, string&gt; connectionProperties = 4;</code>
     */

    public Builder putAllConnectionProperties(
        java.util.Map<String, String> values) {
      internalGetMutableConnectionProperties().getMutableMap()
          .putAll(values);
      return this;
    }
    @Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFieldsProto3(unknownFields);
    }

    @Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:org.polypheny.jdbc.ConnectionRequest)
  }

  // @@protoc_insertion_point(class_scope:org.polypheny.jdbc.ConnectionRequest)
  private static final ConnectionRequest DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new ConnectionRequest();
  }

  public static ConnectionRequest getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ConnectionRequest>
      PARSER = new com.google.protobuf.AbstractParser<ConnectionRequest>() {
    @Override
    public ConnectionRequest parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ConnectionRequest(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ConnectionRequest> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<ConnectionRequest> getParserForType() {
    return PARSER;
  }

  @Override
  public ConnectionRequest getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

