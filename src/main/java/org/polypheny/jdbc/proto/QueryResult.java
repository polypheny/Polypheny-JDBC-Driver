// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Polypheny-JDBC-Driver/src/main/proto/protointerface.proto

package org.polypheny.jdbc.proto;

/**
 * Protobuf type {@code polypheny.protointerface.QueryResult}
 */
public  final class QueryResult extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:polypheny.protointerface.QueryResult)
    QueryResultOrBuilder {
private static final long serialVersionUID = 0L;
  // Use QueryResult.newBuilder() to construct.
  private QueryResult(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private QueryResult() {
  }

  @Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private QueryResult(
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
            resultCase_ = 1;
            result_ = input.readBool();
            break;
          }
          case 18: {
            Frame.Builder subBuilder = null;
            if (resultCase_ == 2) {
              subBuilder = ((Frame) result_).toBuilder();
            }
            result_ =
                input.readMessage( Frame.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom((Frame) result_);
              result_ = subBuilder.buildPartial();
            }
            resultCase_ = 2;
            break;
          }
          case 24: {
            resultCase_ = 3;
            result_ = input.readInt32();
            break;
          }
          case 32: {
            resultCase_ = 4;
            result_ = input.readInt64();
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
    return ProtoInterfaceProto.internal_static_polypheny_protointerface_QueryResult_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return ProtoInterfaceProto.internal_static_polypheny_protointerface_QueryResult_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            QueryResult.class, Builder.class);
  }

  private int resultCase_ = 0;
  private Object result_;
  public enum ResultCase
      implements com.google.protobuf.Internal.EnumLite {
    NORESULT(1),
    FRAME(2),
    COUNT(3),
    BIGCOUNT(4),
    RESULT_NOT_SET(0);
    private final int value;
    private ResultCase(int value) {
      this.value = value;
    }
    /**
     * @deprecated Use {@link #forNumber(int)} instead.
     */
    @Deprecated
    public static ResultCase valueOf(int value) {
      return forNumber(value);
    }

    public static ResultCase forNumber(int value) {
      switch (value) {
        case 1: return NORESULT;
        case 2: return FRAME;
        case 3: return COUNT;
        case 4: return BIGCOUNT;
        case 0: return RESULT_NOT_SET;
        default: return null;
      }
    }
    public int getNumber() {
      return this.value;
    }
  };

  public ResultCase
  getResultCase() {
    return ResultCase.forNumber(
        resultCase_);
  }

  public static final int NORESULT_FIELD_NUMBER = 1;
  /**
   * <code>bool noResult = 1;</code>
   */
  public boolean getNoResult() {
    if (resultCase_ == 1) {
      return (Boolean) result_;
    }
    return false;
  }

  public static final int FRAME_FIELD_NUMBER = 2;
  /**
   * <code>.polypheny.protointerface.Frame frame = 2;</code>
   */
  public boolean hasFrame() {
    return resultCase_ == 2;
  }
  /**
   * <code>.polypheny.protointerface.Frame frame = 2;</code>
   */
  public Frame getFrame() {
    if (resultCase_ == 2) {
       return (Frame) result_;
    }
    return Frame.getDefaultInstance();
  }
  /**
   * <code>.polypheny.protointerface.Frame frame = 2;</code>
   */
  public FrameOrBuilder getFrameOrBuilder() {
    if (resultCase_ == 2) {
       return (Frame) result_;
    }
    return Frame.getDefaultInstance();
  }

  public static final int COUNT_FIELD_NUMBER = 3;
  /**
   * <code>int32 count = 3;</code>
   */
  public int getCount() {
    if (resultCase_ == 3) {
      return (Integer) result_;
    }
    return 0;
  }

  public static final int BIGCOUNT_FIELD_NUMBER = 4;
  /**
   * <code>int64 bigCount = 4;</code>
   */
  public long getBigCount() {
    if (resultCase_ == 4) {
      return (Long) result_;
    }
    return 0L;
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
    if (resultCase_ == 1) {
      output.writeBool(
          1, (boolean)((Boolean) result_));
    }
    if (resultCase_ == 2) {
      output.writeMessage(2, (Frame) result_);
    }
    if (resultCase_ == 3) {
      output.writeInt32(
          3, (int)((Integer) result_));
    }
    if (resultCase_ == 4) {
      output.writeInt64(
          4, (long)((Long) result_));
    }
    unknownFields.writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (resultCase_ == 1) {
      size += com.google.protobuf.CodedOutputStream
        .computeBoolSize(
            1, (boolean)((Boolean) result_));
    }
    if (resultCase_ == 2) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(2, (Frame) result_);
    }
    if (resultCase_ == 3) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(
            3, (int)((Integer) result_));
    }
    if (resultCase_ == 4) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(
            4, (long)((Long) result_));
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
    if (!(obj instanceof QueryResult)) {
      return super.equals(obj);
    }
    QueryResult other = (QueryResult) obj;

    boolean result = true;
    result = result && getResultCase().equals(
        other.getResultCase());
    if (!result) return false;
    switch (resultCase_) {
      case 1:
        result = result && (getNoResult()
            == other.getNoResult());
        break;
      case 2:
        result = result && getFrame()
            .equals(other.getFrame());
        break;
      case 3:
        result = result && (getCount()
            == other.getCount());
        break;
      case 4:
        result = result && (getBigCount()
            == other.getBigCount());
        break;
      case 0:
      default:
    }
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
    switch (resultCase_) {
      case 1:
        hash = (37 * hash) + NORESULT_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getNoResult());
        break;
      case 2:
        hash = (37 * hash) + FRAME_FIELD_NUMBER;
        hash = (53 * hash) + getFrame().hashCode();
        break;
      case 3:
        hash = (37 * hash) + COUNT_FIELD_NUMBER;
        hash = (53 * hash) + getCount();
        break;
      case 4:
        hash = (37 * hash) + BIGCOUNT_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getBigCount());
        break;
      case 0:
      default:
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static QueryResult parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static QueryResult parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static QueryResult parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static QueryResult parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static QueryResult parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static QueryResult parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static QueryResult parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static QueryResult parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static QueryResult parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static QueryResult parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static QueryResult parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static QueryResult parseFrom(
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
  public static Builder newBuilder( QueryResult prototype) {
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
   * Protobuf type {@code polypheny.protointerface.QueryResult}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:polypheny.protointerface.QueryResult)
      QueryResultOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return ProtoInterfaceProto.internal_static_polypheny_protointerface_QueryResult_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return ProtoInterfaceProto.internal_static_polypheny_protointerface_QueryResult_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              QueryResult.class, Builder.class);
    }

    // Construct using org.polypheny.jdbc.proto.QueryResult.newBuilder()
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
      resultCase_ = 0;
      result_ = null;
      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return ProtoInterfaceProto.internal_static_polypheny_protointerface_QueryResult_descriptor;
    }

    @Override
    public QueryResult getDefaultInstanceForType() {
      return QueryResult.getDefaultInstance();
    }

    @Override
    public QueryResult build() {
      QueryResult result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public QueryResult buildPartial() {
      QueryResult result = new QueryResult(this);
      if (resultCase_ == 1) {
        result.result_ = result_;
      }
      if (resultCase_ == 2) {
        if (frameBuilder_ == null) {
          result.result_ = result_;
        } else {
          result.result_ = frameBuilder_.build();
        }
      }
      if (resultCase_ == 3) {
        result.result_ = result_;
      }
      if (resultCase_ == 4) {
        result.result_ = result_;
      }
      result.resultCase_ = resultCase_;
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
      if (other instanceof QueryResult) {
        return mergeFrom((QueryResult)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom( QueryResult other) {
      if (other == QueryResult.getDefaultInstance()) return this;
      switch (other.getResultCase()) {
        case NORESULT: {
          setNoResult(other.getNoResult());
          break;
        }
        case FRAME: {
          mergeFrame(other.getFrame());
          break;
        }
        case COUNT: {
          setCount(other.getCount());
          break;
        }
        case BIGCOUNT: {
          setBigCount(other.getBigCount());
          break;
        }
        case RESULT_NOT_SET: {
          break;
        }
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
      QueryResult parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (QueryResult) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int resultCase_ = 0;
    private Object result_;
    public ResultCase
        getResultCase() {
      return ResultCase.forNumber(
          resultCase_);
    }

    public Builder clearResult() {
      resultCase_ = 0;
      result_ = null;
      onChanged();
      return this;
    }


    /**
     * <code>bool noResult = 1;</code>
     */
    public boolean getNoResult() {
      if (resultCase_ == 1) {
        return (Boolean) result_;
      }
      return false;
    }
    /**
     * <code>bool noResult = 1;</code>
     */
    public Builder setNoResult(boolean value) {
      resultCase_ = 1;
      result_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>bool noResult = 1;</code>
     */
    public Builder clearNoResult() {
      if (resultCase_ == 1) {
        resultCase_ = 0;
        result_ = null;
        onChanged();
      }
      return this;
    }

    private com.google.protobuf.SingleFieldBuilderV3<
        Frame, Frame.Builder, FrameOrBuilder> frameBuilder_;
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public boolean hasFrame() {
      return resultCase_ == 2;
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Frame getFrame() {
      if (frameBuilder_ == null) {
        if (resultCase_ == 2) {
          return (Frame) result_;
        }
        return Frame.getDefaultInstance();
      } else {
        if (resultCase_ == 2) {
          return frameBuilder_.getMessage();
        }
        return Frame.getDefaultInstance();
      }
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Builder setFrame( Frame value) {
      if (frameBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        result_ = value;
        onChanged();
      } else {
        frameBuilder_.setMessage(value);
      }
      resultCase_ = 2;
      return this;
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Builder setFrame(
        Frame.Builder builderForValue) {
      if (frameBuilder_ == null) {
        result_ = builderForValue.build();
        onChanged();
      } else {
        frameBuilder_.setMessage(builderForValue.build());
      }
      resultCase_ = 2;
      return this;
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Builder mergeFrame( Frame value) {
      if (frameBuilder_ == null) {
        if (resultCase_ == 2 &&
            result_ != Frame.getDefaultInstance()) {
          result_ = Frame.newBuilder((Frame) result_)
              .mergeFrom(value).buildPartial();
        } else {
          result_ = value;
        }
        onChanged();
      } else {
        if (resultCase_ == 2) {
          frameBuilder_.mergeFrom(value);
        }
        frameBuilder_.setMessage(value);
      }
      resultCase_ = 2;
      return this;
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Builder clearFrame() {
      if (frameBuilder_ == null) {
        if (resultCase_ == 2) {
          resultCase_ = 0;
          result_ = null;
          onChanged();
        }
      } else {
        if (resultCase_ == 2) {
          resultCase_ = 0;
          result_ = null;
        }
        frameBuilder_.clear();
      }
      return this;
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public Frame.Builder getFrameBuilder() {
      return getFrameFieldBuilder().getBuilder();
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    public FrameOrBuilder getFrameOrBuilder() {
      if ((resultCase_ == 2) && (frameBuilder_ != null)) {
        return frameBuilder_.getMessageOrBuilder();
      } else {
        if (resultCase_ == 2) {
          return (Frame) result_;
        }
        return Frame.getDefaultInstance();
      }
    }
    /**
     * <code>.polypheny.protointerface.Frame frame = 2;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        Frame, Frame.Builder, FrameOrBuilder>
        getFrameFieldBuilder() {
      if (frameBuilder_ == null) {
        if (!(resultCase_ == 2)) {
          result_ = Frame.getDefaultInstance();
        }
        frameBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            Frame, Frame.Builder, FrameOrBuilder>(
                (Frame) result_,
                getParentForChildren(),
                isClean());
        result_ = null;
      }
      resultCase_ = 2;
      onChanged();;
      return frameBuilder_;
    }

    /**
     * <code>int32 count = 3;</code>
     */
    public int getCount() {
      if (resultCase_ == 3) {
        return (Integer) result_;
      }
      return 0;
    }
    /**
     * <code>int32 count = 3;</code>
     */
    public Builder setCount(int value) {
      resultCase_ = 3;
      result_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int32 count = 3;</code>
     */
    public Builder clearCount() {
      if (resultCase_ == 3) {
        resultCase_ = 0;
        result_ = null;
        onChanged();
      }
      return this;
    }

    /**
     * <code>int64 bigCount = 4;</code>
     */
    public long getBigCount() {
      if (resultCase_ == 4) {
        return (Long) result_;
      }
      return 0L;
    }
    /**
     * <code>int64 bigCount = 4;</code>
     */
    public Builder setBigCount(long value) {
      resultCase_ = 4;
      result_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int64 bigCount = 4;</code>
     */
    public Builder clearBigCount() {
      if (resultCase_ == 4) {
        resultCase_ = 0;
        result_ = null;
        onChanged();
      }
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


    // @@protoc_insertion_point(builder_scope:polypheny.protointerface.QueryResult)
  }

  // @@protoc_insertion_point(class_scope:polypheny.protointerface.QueryResult)
  private static final QueryResult DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new QueryResult();
  }

  public static QueryResult getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<QueryResult>
      PARSER = new com.google.protobuf.AbstractParser<QueryResult>() {
    @Override
    public QueryResult parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new QueryResult(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<QueryResult> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<QueryResult> getParserForType() {
    return PARSER;
  }

  @Override
  public QueryResult getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

