java_library(
    name = "bazel_env",
    srcs = ["BazelEnv.java"],
    deps = [
        "//src/main/java/com/google/devtools/build/lib:runtime",
        "//src/main/java/com/google/devtools/build/lib/authandtls",
        "//src/main/java/com/google/devtools/build/lib/bazel:main",
        "//src/main/java/com/google/devtools/build/lib/events",
        "//src/main/java/com/google/devtools/build/lib/remote",
        "//src/main/java/com/google/devtools/build/lib/remote/common",
        "//src/main/java/com/google/devtools/build/lib/remote/options",
        "//src/main/java/com/google/devtools/build/lib/remote/util",
        "//src/main/java/com/google/devtools/build/lib/util:detailed_exit_code",
        "//src/main/java/com/google/devtools/build/lib/vfs",
        "//src/main/java/com/google/devtools/common/options",
        "//src/main/java/com/google/devtools/common/options:invocation_policy",
        "//src/main/protobuf:failure_details_java_proto",
        "//src/main/protobuf:invocation_policy_java_proto",
        "//src/main/protobuf:invocation_policy_proto",
        "//third_party:auth",
        "//third_party:flogger",
        "//third_party:guava",
        "//third_party/grpc:grpc-jar",
        "@remoteapis//:build_bazel_remote_execution_v2_remote_execution_java_grpc",
        "@remoteapis//:build_bazel_remote_execution_v2_remote_execution_java_proto",
    ],
)

java_binary(
    name = "upload_file",
    srcs = ["UploadFile.java"],
    main_class = "com.google.devtools.build.tools.UploadFile",
    deps = [":bazel_env"],
)

java_binary(
    name = "download_file",
    srcs = ["DownloadFile.java"],
    main_class = "com.google.devtools.build.tools.DownloadFile",
    deps = [":bazel_env"],
)
