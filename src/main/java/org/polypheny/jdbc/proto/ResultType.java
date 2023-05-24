package org.polypheny.jdbc.proto;// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Polypheny-JDBC-Driver/src/main/proto/protointerface.proto

/**
 * Protobuf enum {@code org.polypheny.jdbc.ResultType}
 */
public enum ResultType
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>NONE = 0;</code>
   */
  NONE(0),
  /**
   * <code>FRAME = 1;</code>
   */
  FRAME(1),
  /**
   * <code>COUNT = 2;</code>
   */
  COUNT(2),
  /**
   * <code>BIG_COUNT = 3;</code>
   */
  BIG_COUNT(3),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>NONE = 0;</code>
   */
  public static final int NONE_VALUE = 0;
  /**
   * <code>FRAME = 1;</code>
   */
  public static final int FRAME_VALUE = 1;
  /**
   * <code>COUNT = 2;</code>
   */
  public static final int COUNT_VALUE = 2;
  /**
   * <code>BIG_COUNT = 3;</code>
   */
  public static final int BIG_COUNT_VALUE = 3;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @Deprecated
  public static ResultType valueOf(int value) {
    return forNumber(value);
  }

  public static ResultType forNumber(int value) {
    switch (value) {
      case 0: return NONE;
      case 1: return FRAME;
      case 2: return COUNT;
      case 3: return BIG_COUNT;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<ResultType>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      ResultType> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<ResultType>() {
          public ResultType findValueByNumber(int number) {
            return ResultType.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return ProtoInterfaceProto.getDescriptor().getEnumTypes().get(0);
  }

  private static final ResultType[] VALUES = values();

  public static ResultType valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private ResultType(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:org.polypheny.jdbc.ResultType)
}
