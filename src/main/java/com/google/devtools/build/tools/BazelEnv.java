package com.google.devtools.build.tools;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import com.google.devtools.build.lib.authandtls.CallCredentialsProvider;
import com.google.devtools.build.lib.authandtls.GoogleAuthUtils;
import com.google.devtools.build.lib.bazel.Bazel;
import com.google.devtools.build.lib.events.StoredEventHandler;
import com.google.devtools.build.lib.remote.*;
import com.google.devtools.build.lib.remote.common.RemoteActionExecutionContext;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.runtime.*;
import com.google.devtools.build.lib.runtime.proto.InvocationPolicyOuterClass;
import com.google.devtools.build.lib.util.DetailedExitCode;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.common.options.InvocationPolicyParser;
import com.google.devtools.common.options.OpaqueOptionsData;
import com.google.devtools.common.options.OptionsParser;
import com.google.devtools.common.options.OptionsParsingResult;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.google.devtools.common.options.Converters.BLAZE_ALIASING_FLAG;

public class BazelEnv {
    private final ListeningScheduledExecutorService retryScheduler =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(1));
    private final GrpcCacheClient cacheClient;
    private final FileSystem fileSystem;
    private final DigestUtil digestUtil;
    private final BlazeRuntime runtime;
    private final ChannelFactory channelFactory =
            new ChannelFactory() {
                @Override
                public ManagedChannel newChannel(
                        String target,
                        String proxy,
                        AuthAndTLSOptions options,
                        List<ClientInterceptor> interceptors)
                        throws IOException {
                    return GoogleAuthUtils.newChannel(
                            target, proxy, options, interceptors.isEmpty() ? null : interceptors);
                }
            };
    private final LoadingCache<BlazeCommand, OpaqueOptionsData> optionsDataCache =
            CacheBuilder.newBuilder()
                    .build(
                            new CacheLoader<BlazeCommand, OpaqueOptionsData>() {
                                @Override
                                public OpaqueOptionsData load(BlazeCommand command) {
                                    return OptionsParser.getOptionsData(
                                            BlazeCommandUtils.getOptions(
                                                    command.getClass(),
                                                    runtime.getBlazeModules(),
                                                    runtime.getRuleClassProvider()));
                                }
                            });

    private OptionsParser createOptionsParser(BlazeCommand command)
            throws OptionsParser.ConstructionException {
        OpaqueOptionsData optionsData;
        try {
            optionsData = optionsDataCache.getUnchecked(command);
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), OptionsParser.ConstructionException.class);
            throw new IllegalStateException(e);
        }
        Command annotation = command.getClass().getAnnotation(Command.class);
        OptionsParser parser =
                OptionsParser.builder()
                        .optionsData(optionsData)
                        .skipStarlarkOptionPrefixes()
                        .allowResidue(annotation.allowResidue())
                        .withAliasFlag(BLAZE_ALIASING_FLAG)
                        .build();
        return parser;
    }

    BazelEnv(List<String> commandArgs) throws Exception {
        String workspaceDir = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspaceDir == null) {
            throw new IllegalArgumentException("BUILD_WORKSPACE_DIRECTORY is not set");
        }

        new File("/tmp/bazelenv").mkdir();

        // These flags don't matter very much for the tools but need to be set to init Bazel.
        List<String> startupArgs = new ArrayList<>();
        startupArgs.add("--failure_detail_out=/tmp/failure_detail.rawproto");
        startupArgs.add("--install_base=/tmp/bazelenv");
        startupArgs.add("--output_base=/tmp/blazeenv-out");
        startupArgs.add("--output_user_root=/tmp/blaze-out");
        startupArgs.add("--workspace_directory=" + workspaceDir);

        List<BlazeModule> modules = BlazeRuntime.createModules(Bazel.BAZEL_MODULES);
        this.runtime = BlazeRuntime.newRuntime(modules, startupArgs, new Runnable() {
            @Override
            public void run() {
                System.out.println("Prepare for abrupt shutdown");
            }
        });

        BlazeCommand command = runtime.getCommandMap().get("build");
        Command commandAnnotation = command.getClass().getAnnotation(Command.class);
        BlazeWorkspace workspace = runtime.getWorkspace();

        InvocationPolicyOuterClass.InvocationPolicy invocationPolicy = InvocationPolicyParser.parsePolicy("");
        BlazeOptionHandler optionHandler =
                new BlazeOptionHandler(
                        runtime,
                        workspace,
                        command,
                        commandAnnotation,
                        // Provide the options parser so that we can cache OptionsData here.
                        createOptionsParser(command),
                        invocationPolicy);
        StoredEventHandler storedEventHandler = new StoredEventHandler();
        ImmutableList<String> realCommandArgs = ImmutableList.<String>builder()
                .add("build")
                .addAll(commandArgs)
                .build();
        DetailedExitCode earlyExitCode = optionHandler.parseOptions(realCommandArgs, storedEventHandler);
        if (!earlyExitCode.isSuccess()) {
            throw new RuntimeException("Could not setup options: " + earlyExitCode.getFailureDetail());
        }

        OptionsParsingResult options = optionHandler.getOptionsResult();

        fileSystem = runtime.getFileSystem();

        RemoteOptions remoteOptions = options.getOptions(RemoteOptions.class);
        AuthAndTLSOptions authAndTlsOptions = new AuthAndTLSOptions();
        digestUtil = new DigestUtil(fileSystem.getDigestFunction());

        int maxConcurrencyPerConnection = 100;

        if (remoteOptions.remoteCache == null || remoteOptions.remoteCache.equals("")) {
           throw new IllegalArgumentException("--remote_cache is not set");
        }

        ImmutableList.Builder<ClientInterceptor> interceptors = ImmutableList.builder();
        interceptors.add(TracingMetadataUtils.newCacheHeadersInterceptor(remoteOptions));
        ReferenceCountedChannel cacheChannel = new ReferenceCountedChannel(
                new GoogleChannelConnectionFactory(
                        channelFactory,
                        remoteOptions.remoteCache,
                        remoteOptions.remoteProxy,
                        authAndTlsOptions,
                        interceptors.build(),
                        maxConcurrencyPerConnection));

        CallCredentialsProvider callCredentialsProvider = GoogleAuthUtils.newCallCredentialsProvider(
                GoogleAuthUtils.newCredentials(authAndTlsOptions));

        RemoteRetrier retrier =
                new RemoteRetrier(
                        remoteOptions,
                        RemoteRetrier.RETRIABLE_GRPC_ERRORS,
                        retryScheduler,
                        Retrier.ALLOW_ALL_CALLS);

        ByteStreamUploader uploader =
                new ByteStreamUploader(
                        remoteOptions.remoteInstanceName,
                        cacheChannel.retain(),
                        callCredentialsProvider,
                        remoteOptions.remoteTimeout.getSeconds(),
                        retrier);

        cacheClient =
                new GrpcCacheClient(
                        cacheChannel.retain(),
                        callCredentialsProvider,
                        remoteOptions,
                        retrier,
                        digestUtil,
                        uploader.retain());

    }

    void uploadFile(File inputFile) throws Exception {
        System.out.println("Uploading file " + inputFile.getAbsolutePath());
        RequestMetadata metadata =
                TracingMetadataUtils.buildMetadata(
                        "buildid", "commandid", "actionid", null);
        RemoteActionExecutionContext remoteActionExecutionContext =
                RemoteActionExecutionContext.create(metadata);
        Path path = fileSystem.getPath(inputFile.getAbsolutePath());
        Digest digest = digestUtil.compute(path);
        System.out.println("File digest: " + digest.getHash() + "/" + digest.getSizeBytes());

        LocalDateTime start = LocalDateTime.now();
        cacheClient.uploadFile(remoteActionExecutionContext, digest, path).get();
        Duration elapsed = Duration.between(start, LocalDateTime.now());
        System.out.println("Finished uploading file in " + elapsed);
    }

    void downloadFile(String hashAndSize) throws Exception {
        System.out.println("Downloading digest " + hashAndSize);
        RequestMetadata metadata =
                TracingMetadataUtils.buildMetadata(
                        "buildid", "commandid", "actionid", null);
        RemoteActionExecutionContext remoteActionExecutionContext =
                RemoteActionExecutionContext.create(metadata);

        String[] parts = hashAndSize.split("/");
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid digest " + hashAndSize);
        }
        Digest digest = DigestUtil.buildDigest(parts[0], Long.parseLong(parts[1]));
        LocalDateTime start = LocalDateTime.now();
        cacheClient.downloadBlob(remoteActionExecutionContext, digest, OutputStream.nullOutputStream()).get();
        Duration elapsed = Duration.between(start, LocalDateTime.now());
        System.out.println("Finished downloading file in " + elapsed);
    }
}
