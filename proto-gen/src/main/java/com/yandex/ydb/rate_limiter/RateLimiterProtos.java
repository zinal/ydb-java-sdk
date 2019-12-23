// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: kikimr/public/api/protos/ydb_rate_limiter.proto

package tech.ydb.rate_limiter;

public final class RateLimiterProtos {
  private RateLimiterProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_HierarchicalDrrSettings_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_HierarchicalDrrSettings_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_Resource_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_Resource_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_CreateResourceRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_CreateResourceRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_CreateResourceResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_CreateResourceResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_CreateResourceResult_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_CreateResourceResult_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_AlterResourceRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_AlterResourceRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_AlterResourceResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_AlterResourceResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_AlterResourceResult_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_AlterResourceResult_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DropResourceRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DropResourceRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DropResourceResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DropResourceResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DropResourceResult_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DropResourceResult_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_ListResourcesRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_ListResourcesRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_ListResourcesResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_ListResourcesResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_ListResourcesResult_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_ListResourcesResult_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DescribeResourceRequest_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DescribeResourceRequest_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DescribeResourceResponse_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_Ydb_RateLimiter_DescribeResourceResult_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_Ydb_RateLimiter_DescribeResourceResult_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n/kikimr/public/api/protos/ydb_rate_limi" +
      "ter.proto\022\017Ydb.RateLimiter\032,kikimr/publi" +
      "c/api/protos/ydb_operation.proto\"[\n\027Hier" +
      "archicalDrrSettings\022\034\n\024max_units_per_sec" +
      "ond\030\001 \001(\001\022\"\n\032max_burst_size_coefficient\030" +
      "\002 \001(\001\"o\n\010Resource\022\025\n\rresource_path\030\001 \001(\t" +
      "\022D\n\020hierarchical_drr\030\002 \001(\0132(.Ydb.RateLim" +
      "iter.HierarchicalDrrSettingsH\000B\006\n\004type\"\237" +
      "\001\n\025CreateResourceRequest\0229\n\020operation_pa" +
      "rams\030\001 \001(\0132\037.Ydb.Operations.OperationPar",
      "ams\022\036\n\026coordination_node_path\030\002 \001(\t\022+\n\010r" +
      "esource\030\003 \001(\0132\031.Ydb.RateLimiter.Resource" +
      "\"F\n\026CreateResourceResponse\022,\n\toperation\030" +
      "\001 \001(\0132\031.Ydb.Operations.Operation\"\026\n\024Crea" +
      "teResourceResult\"\236\001\n\024AlterResourceReques" +
      "t\0229\n\020operation_params\030\001 \001(\0132\037.Ydb.Operat" +
      "ions.OperationParams\022\036\n\026coordination_nod" +
      "e_path\030\002 \001(\t\022+\n\010resource\030\003 \001(\0132\031.Ydb.Rat" +
      "eLimiter.Resource\"E\n\025AlterResourceRespon" +
      "se\022,\n\toperation\030\001 \001(\0132\031.Ydb.Operations.O",
      "peration\"\025\n\023AlterResourceResult\"\207\001\n\023Drop" +
      "ResourceRequest\0229\n\020operation_params\030\001 \001(" +
      "\0132\037.Ydb.Operations.OperationParams\022\036\n\026co" +
      "ordination_node_path\030\002 \001(\t\022\025\n\rresource_p" +
      "ath\030\003 \001(\t\"D\n\024DropResourceResponse\022,\n\tope" +
      "ration\030\001 \001(\0132\031.Ydb.Operations.Operation\"" +
      "\024\n\022DropResourceResult\"\233\001\n\024ListResourcesR" +
      "equest\0229\n\020operation_params\030\001 \001(\0132\037.Ydb.O" +
      "perations.OperationParams\022\036\n\026coordinatio" +
      "n_node_path\030\002 \001(\t\022\025\n\rresource_path\030\003 \001(\t",
      "\022\021\n\trecursive\030\004 \001(\010\"E\n\025ListResourcesResp" +
      "onse\022,\n\toperation\030\001 \001(\0132\031.Ydb.Operations" +
      ".Operation\"-\n\023ListResourcesResult\022\026\n\016res" +
      "ource_paths\030\001 \003(\t\"\213\001\n\027DescribeResourceRe" +
      "quest\0229\n\020operation_params\030\001 \001(\0132\037.Ydb.Op" +
      "erations.OperationParams\022\036\n\026coordination" +
      "_node_path\030\002 \001(\t\022\025\n\rresource_path\030\003 \001(\t\"" +
      "H\n\030DescribeResourceResponse\022,\n\toperation" +
      "\030\001 \001(\0132\031.Ydb.Operations.Operation\"E\n\026Des" +
      "cribeResourceResult\022+\n\010resource\030\001 \001(\0132\031.",
      "Ydb.RateLimiter.ResourceB5\n\033com.yandex.y" +
      "db.rate_limiterB\021RateLimiterProtosP\001\370\001\001b" +
      "\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          tech.ydb.OperationProtos.getDescriptor(),
        }, assigner);
    internal_static_Ydb_RateLimiter_HierarchicalDrrSettings_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_Ydb_RateLimiter_HierarchicalDrrSettings_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_HierarchicalDrrSettings_descriptor,
        new java.lang.String[] { "MaxUnitsPerSecond", "MaxBurstSizeCoefficient", });
    internal_static_Ydb_RateLimiter_Resource_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_Ydb_RateLimiter_Resource_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_Resource_descriptor,
        new java.lang.String[] { "ResourcePath", "HierarchicalDrr", "Type", });
    internal_static_Ydb_RateLimiter_CreateResourceRequest_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_Ydb_RateLimiter_CreateResourceRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_CreateResourceRequest_descriptor,
        new java.lang.String[] { "OperationParams", "CoordinationNodePath", "Resource", });
    internal_static_Ydb_RateLimiter_CreateResourceResponse_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_Ydb_RateLimiter_CreateResourceResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_CreateResourceResponse_descriptor,
        new java.lang.String[] { "Operation", });
    internal_static_Ydb_RateLimiter_CreateResourceResult_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_Ydb_RateLimiter_CreateResourceResult_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_CreateResourceResult_descriptor,
        new java.lang.String[] { });
    internal_static_Ydb_RateLimiter_AlterResourceRequest_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_Ydb_RateLimiter_AlterResourceRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_AlterResourceRequest_descriptor,
        new java.lang.String[] { "OperationParams", "CoordinationNodePath", "Resource", });
    internal_static_Ydb_RateLimiter_AlterResourceResponse_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_Ydb_RateLimiter_AlterResourceResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_AlterResourceResponse_descriptor,
        new java.lang.String[] { "Operation", });
    internal_static_Ydb_RateLimiter_AlterResourceResult_descriptor =
      getDescriptor().getMessageTypes().get(7);
    internal_static_Ydb_RateLimiter_AlterResourceResult_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_AlterResourceResult_descriptor,
        new java.lang.String[] { });
    internal_static_Ydb_RateLimiter_DropResourceRequest_descriptor =
      getDescriptor().getMessageTypes().get(8);
    internal_static_Ydb_RateLimiter_DropResourceRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DropResourceRequest_descriptor,
        new java.lang.String[] { "OperationParams", "CoordinationNodePath", "ResourcePath", });
    internal_static_Ydb_RateLimiter_DropResourceResponse_descriptor =
      getDescriptor().getMessageTypes().get(9);
    internal_static_Ydb_RateLimiter_DropResourceResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DropResourceResponse_descriptor,
        new java.lang.String[] { "Operation", });
    internal_static_Ydb_RateLimiter_DropResourceResult_descriptor =
      getDescriptor().getMessageTypes().get(10);
    internal_static_Ydb_RateLimiter_DropResourceResult_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DropResourceResult_descriptor,
        new java.lang.String[] { });
    internal_static_Ydb_RateLimiter_ListResourcesRequest_descriptor =
      getDescriptor().getMessageTypes().get(11);
    internal_static_Ydb_RateLimiter_ListResourcesRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_ListResourcesRequest_descriptor,
        new java.lang.String[] { "OperationParams", "CoordinationNodePath", "ResourcePath", "Recursive", });
    internal_static_Ydb_RateLimiter_ListResourcesResponse_descriptor =
      getDescriptor().getMessageTypes().get(12);
    internal_static_Ydb_RateLimiter_ListResourcesResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_ListResourcesResponse_descriptor,
        new java.lang.String[] { "Operation", });
    internal_static_Ydb_RateLimiter_ListResourcesResult_descriptor =
      getDescriptor().getMessageTypes().get(13);
    internal_static_Ydb_RateLimiter_ListResourcesResult_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_ListResourcesResult_descriptor,
        new java.lang.String[] { "ResourcePaths", });
    internal_static_Ydb_RateLimiter_DescribeResourceRequest_descriptor =
      getDescriptor().getMessageTypes().get(14);
    internal_static_Ydb_RateLimiter_DescribeResourceRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DescribeResourceRequest_descriptor,
        new java.lang.String[] { "OperationParams", "CoordinationNodePath", "ResourcePath", });
    internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor =
      getDescriptor().getMessageTypes().get(15);
    internal_static_Ydb_RateLimiter_DescribeResourceResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DescribeResourceResponse_descriptor,
        new java.lang.String[] { "Operation", });
    internal_static_Ydb_RateLimiter_DescribeResourceResult_descriptor =
      getDescriptor().getMessageTypes().get(16);
    internal_static_Ydb_RateLimiter_DescribeResourceResult_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_Ydb_RateLimiter_DescribeResourceResult_descriptor,
        new java.lang.String[] { "Resource", });
    tech.ydb.OperationProtos.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
