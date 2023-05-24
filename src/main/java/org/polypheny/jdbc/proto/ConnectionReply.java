package org.polypheny.jdbc.proto;// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Polypheny-JDBC-Driver/src/main/proto/protointerface.proto

/**
 * <pre>
 * Sent by the server as a response to a ConnectionRequest.
 * </pre>
 *
 * Protobuf type {@code org.polypheny.jdbc.ConnectionReply}
 */
public  final class ConnectionReply extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:org.polypheny.jdbc.ConnectionReply)
        ConnectionReplyOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ConnectionReply.newBuilder() to construct.
  private ConnectionReply(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ConnectionReply() {
    isCompatible_ = false;
    majorApiVersion_ = 0;
    minorApiVersion_ = 0;
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ConnectionReply(
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

            isCompatible_ = input.readBool();
            break;
          }
          case 16: {

            majorApiVersion_ = input.readUInt32();
            break;
          }
          case 24: {

            minorApiVersion_ = input.readUInt32();
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
    return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionReply_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionReply_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            ConnectionReply.class, Builder.class);
  }

  public static final int ISCOMPATIBLE_FIELD_NUMBER = 1;
  private boolean isCompatible_;
  /**
   * <code>bool isCompatible = 1;</code>
   */
  public boolean getIsCompatible() {
    return isCompatible_;
  }

  public static final int MAJORAPIVERSION_FIELD_NUMBER = 2;
  private int majorApiVersion_;
  /**
   * <code>uint32 majorApiVersion = 2;</code>
   */
  public int getMajorApiVersion() {
    return majorApiVersion_;
  }

  public static final int MINORAPIVERSION_FIELD_NUMBER = 3;
  private int minorApiVersion_;
  /**
   * <code>uint32 minorApiVersion = 3;</code>
   */
  public int getMinorApiVersion() {
    return minorApiVersion_;
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
    if (isCompatible_ != false) {
      output.writeBool(1, isCompatible_);
    }
    if (majorApiVersion_ != 0) {
      output.writeUInt32(2, majorApiVersion_);
    }
    if (minorApiVersion_ != 0) {
      output.writeUInt32(3, minorApiVersion_);
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (isCompatible_ != false) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(1, isCompatible_);
    }
    if (majorApiVersion_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeUInt32Size(2, majorApiVersion_);
    }
    if (minorApiVersion_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeUInt32Size(3, minorApiVersion_);
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
    if (!(obj instanceof ConnectionReply)) {
      return super.equals(obj);
    }
    ConnectionReply other = (ConnectionReply) obj;

    boolean result = true;
    result = result && (getIsCompatible()
        == other.getIsCompatible());
    result = result && (getMajorApiVersion()
        == other.getMajorApiVersion());
    result = result && (getMinorApiVersion()
        == other.getMinorApiVersion());
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
    hash = (37 * hash) + ISCOMPATIBLE_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
        getIsCompatible());
    hash = (37 * hash) + MAJORAPIVERSION_FIELD_NUMBER;
    hash = (53 * hash) + getMajorApiVersion();
    hash = (37 * hash) + MINORAPIVERSION_FIELD_NUMBER;
    hash = (53 * hash) + getMinorApiVersion();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static ConnectionReply parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionReply parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionReply parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionReply parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionReply parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static ConnectionReply parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static ConnectionReply parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ConnectionReply parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static ConnectionReply parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static ConnectionReply parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static ConnectionReply parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static ConnectionReply parseFrom(
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
  public static Builder newBuilder(ConnectionReply prototype) {
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
   * Sent by the server as a response to a ConnectionRequest.
   * </pre>
   *
   * Protobuf type {@code org.polypheny.jdbc.ConnectionReply}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:org.polypheny.jdbc.ConnectionReply)
          ConnectionReplyOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionReply_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionReply_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              ConnectionReply.class, Builder.class);
    }

    // Construct using ConnectionReply.newBuilder()
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
      isCompatible_ = false;

      majorApiVersion_ = 0;

      minorApiVersion_ = 0;

      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return ProtoInterfaceProto.internal_static_org_polypheny_jdbc_ConnectionReply_descriptor;
    }

    @Override
    public ConnectionReply getDefaultInstanceForType() {
      return ConnectionReply.getDefaultInstance();
    }

    @Override
    public ConnectionReply build() {
      ConnectionReply result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public ConnectionReply buildPartial() {
      ConnectionReply result = new ConnectionReply(this);
      result.isCompatible_ = isCompatible_;
      result.majorApiVersion_ = majorApiVersion_;
      result.minorApiVersion_ = minorApiVersion_;
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
      if (other instanceof ConnectionReply) {
        return mergeFrom((ConnectionReply)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(ConnectionReply other) {
      if (other == ConnectionReply.getDefaultInstance()) return this;
      if (other.getIsCompatible() != false) {
        setIsCompatible(other.getIsCompatible());
      }
      if (other.getMajorApiVersion() != 0) {
        setMajorApiVersion(other.getMajorApiVersion());
      }
      if (other.getMinorApiVersion() != 0) {
        setMinorApiVersion(other.getMinorApiVersion());
      }
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
      ConnectionReply parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (ConnectionReply) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private boolean isCompatible_ ;
    /**
     * <code>bool isCompatible = 1;</code>
     */
    public boolean getIsCompatible() {
      return isCompatible_;
    }
    /**
     * <code>bool isCompatible = 1;</code>
     */
    public Builder setIsCompatible(boolean value) {
      
      isCompatible_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>bool isCompatible = 1;</code>
     */
    public Builder clearIsCompatible() {
      
      isCompatible_ = false;
      onChanged();
      return this;
    }

    private int majorApiVersion_ ;
    /**
     * <code>uint32 majorApiVersion = 2;</code>
     */
    public int getMajorApiVersion() {
      return majorApiVersion_;
    }
    /**
     * <code>uint32 majorApiVersion = 2;</code>
     */
    public Builder setMajorApiVersion(int value) {
      
      majorApiVersion_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>uint32 majorApiVersion = 2;</code>
     */
    public Builder clearMajorApiVersion() {
      
      majorApiVersion_ = 0;
      onChanged();
      return this;
    }

    private int minorApiVersion_ ;
    /**
     * <code>uint32 minorApiVersion = 3;</code>
     */
    public int getMinorApiVersion() {
      return minorApiVersion_;
    }
    /**
     * <code>uint32 minorApiVersion = 3;</code>
     */
    public Builder setMinorApiVersion(int value) {
      
      minorApiVersion_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>uint32 minorApiVersion = 3;</code>
     */
    public Builder clearMinorApiVersion() {
      
      minorApiVersion_ = 0;
      onChanged();
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


    // @@protoc_insertion_point(builder_scope:org.polypheny.jdbc.ConnectionReply)
  }

  // @@protoc_insertion_point(class_scope:org.polypheny.jdbc.ConnectionReply)
  private static final ConnectionReply DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new ConnectionReply();
  }

  public static ConnectionReply getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ConnectionReply>
      PARSER = new com.google.protobuf.AbstractParser<ConnectionReply>() {
    @Override
    public ConnectionReply parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ConnectionReply(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ConnectionReply> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<ConnectionReply> getParserForType() {
    return PARSER;
  }

  @Override
  public ConnectionReply getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
