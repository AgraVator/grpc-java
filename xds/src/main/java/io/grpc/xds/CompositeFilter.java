/*
 * Copyright 2026 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import com.github.udpa.udpa.type.v1.TypedStruct;
import com.github.xds.core.v3.TypedExtensionConfig;
import com.github.xds.type.matcher.v3.Matcher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MetricRecorder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import io.grpc.xds.internal.matcher.MatchContext;
import io.grpc.xds.internal.matcher.MatchResult;
import io.grpc.xds.internal.matcher.UnifiedMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.Nullable;

final class CompositeFilter implements Filter {

  static final String TYPE_URL_EXTENSION_WITH_MATCHER =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcher";
  static final String TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute";

  @Nullable
  private final MetricRecorder metricsRecorder;

  CompositeFilter(@Nullable MetricRecorder metricsRecorder) {
    this.metricsRecorder = metricsRecorder;
  }

  static final class Provider implements Filter.Provider {

    private final Function<String, Filter.Provider> registryLookup;

    Provider() {
      this(typeUrl -> FilterRegistry.getDefaultRegistry().get(typeUrl));
    }

    @VisibleForTesting
    Provider(Function<String, Filter.Provider> registryLookup) {
      this.registryLookup = Preconditions.checkNotNull(registryLookup, "registryLookup");
    }

    @Override
    public String[] typeUrls() {
      return new String[] {
          TYPE_URL_EXTENSION_WITH_MATCHER,
          TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE
      };
    }

    @Override
    public boolean isClientFilter() {
      return true;
    }

    @Override
    public boolean isServerFilter() {
      return true;
    }

    @Override
    public Filter newInstance(FilterContext context) {
      return new CompositeFilter(context != null ? context.metricsRecorder() : null);
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfig(
        Message rawProtoMessage, FilterConfigParseContext context) {
      if (!isSupported()) {
        return ConfigOrError.fromError(
            "Composite Filter is experimental and disabled by default.");
      }
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError(
            "Invalid message type: " + rawProtoMessage.getClass().getName());
      }
      int depth = context.recursionDepth() != null ? context.recursionDepth() : 0;
      if (depth >= 8) {
        return ConfigOrError.fromError("Maximum recursion depth of 8 exceeded");
      }
      try {
        Any any = (Any) rawProtoMessage;
        if (!any.is(ExtensionWithMatcher.class)) {
          return ConfigOrError.fromError(
              "Expected ExtensionWithMatcher but got: " + any.getTypeUrl());
        }
        ExtensionWithMatcher proto = any.unpack(ExtensionWithMatcher.class);
        if (!proto.hasExtensionConfig()
            || !proto.getExtensionConfig().hasTypedConfig()
            || !proto.getExtensionConfig().getTypedConfig().is(Composite.class)) {
          return ConfigOrError.fromError(
              "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
        }
        return parseMatcherConfig(proto.hasXdsMatcher() ? proto.getXdsMatcher() : null, context);
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfigOverride(
        Message rawProtoMessage, FilterConfigParseContext context) {
      if (!isSupported()) {
        return ConfigOrError.fromError(
            "Composite Filter is experimental and disabled by default.");
      }
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError(
            "Invalid message type: " + rawProtoMessage.getClass().getName());
      }
      int depth = context.recursionDepth() != null ? context.recursionDepth() : 0;
      if (depth >= 8) {
        return ConfigOrError.fromError("Maximum recursion depth of 8 exceeded");
      }
      try {
        Any any = (Any) rawProtoMessage;
        if (!any.is(ExtensionWithMatcherPerRoute.class)) {
          return ConfigOrError.fromError(
              "Expected ExtensionWithMatcherPerRoute but got: " + any.getTypeUrl());
        }
        ExtensionWithMatcherPerRoute proto = any.unpack(ExtensionWithMatcherPerRoute.class);
        return parseMatcherConfig(proto.hasXdsMatcher() ? proto.getXdsMatcher() : null, context);
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }
    }

    private ConfigOrError<CompositeFilterConfig> parseMatcherConfig(
        @Nullable Matcher matcherProto, FilterConfigParseContext context) {
      if (matcherProto == null) {
        return ConfigOrError.fromConfig(
            new CompositeFilterConfig(null, Collections.emptyMap()));
      }
      if (hasKeepMatching(matcherProto)) {
        return ConfigOrError.fromError(
            "keep_matching is not permitted anywhere in the composite filter matcher tree");
      }
      try {
        Map<String, FilterDelegate> delegates = new HashMap<>();
        collectDelegates(matcherProto, delegates, context);
        UnifiedMatcher matcher = UnifiedMatcher.fromProto(
            matcherProto, this::validateActionTypeUrl);
        return ConfigOrError.fromConfig(new CompositeFilterConfig(matcher, delegates));
      } catch (Exception e) {
        return ConfigOrError.fromError("Failed to create matcher: " + e.getMessage());
      }
    }

    private static boolean hasKeepMatching(Matcher matcher) {
      if (matcher.hasMatcherList()) {
        for (Matcher.MatcherList.FieldMatcher fm : matcher.getMatcherList().getMatchersList()) {
          if (fm.hasOnMatch() && hasKeepMatching(fm.getOnMatch())) {
            return true;
          }
        }
      } else if (matcher.hasMatcherTree()) {
        Matcher.MatcherTree tree = matcher.getMatcherTree();
        if (tree.hasExactMatchMap()) {
          for (Matcher.OnMatch om : tree.getExactMatchMap().getMapMap().values()) {
            if (hasKeepMatching(om)) {
              return true;
            }
          }
        } else if (tree.hasPrefixMatchMap()) {
          for (Matcher.OnMatch om : tree.getPrefixMatchMap().getMapMap().values()) {
            if (hasKeepMatching(om)) {
              return true;
            }
          }
        }
      }
      if (matcher.hasOnNoMatch() && hasKeepMatching(matcher.getOnNoMatch())) {
        return true;
      }
      return false;
    }

    private static boolean hasKeepMatching(Matcher.OnMatch onMatch) {
      if (onMatch.getKeepMatching()) {
        return true;
      }
      if (onMatch.hasMatcher()) {
        return hasKeepMatching(onMatch.getMatcher());
      }
      return false;
    }

    private boolean validateActionTypeUrl(String typeUrl) {
      return "type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3.SkipFilter"
          .equals(typeUrl)
          || "type.googleapis.com/envoy.extensions.filters.http.composite.v3.ExecuteFilterAction"
          .equals(typeUrl);
    }

    private void collectDelegates(Matcher matcher, Map<String, FilterDelegate> map,
        FilterConfigParseContext context) {
      if (matcher.hasMatcherList()) {
        for (Matcher.MatcherList.FieldMatcher fm : matcher.getMatcherList().getMatchersList()) {
          if (fm.hasOnMatch()) {
            collectOnMatch(fm.getOnMatch(), map, context);
          }
        }
      } else if (matcher.hasMatcherTree()) {
        Matcher.MatcherTree tree = matcher.getMatcherTree();
        if (tree.hasExactMatchMap()) {
          for (Matcher.OnMatch om : tree.getExactMatchMap().getMapMap().values()) {
            collectOnMatch(om, map, context);
          }
        } else if (tree.hasPrefixMatchMap()) {
          for (Matcher.OnMatch om : tree.getPrefixMatchMap().getMapMap().values()) {
            collectOnMatch(om, map, context);
          }
        }
      }
      if (matcher.hasOnNoMatch()) {
        collectOnMatch(matcher.getOnNoMatch(), map, context);
      }
    }

    private void collectOnMatch(Matcher.OnMatch onMatch, Map<String, FilterDelegate> map,
        FilterConfigParseContext context) {
      if (onMatch.hasMatcher()) {
        collectDelegates(onMatch.getMatcher(), map, context);
      } else if (onMatch.hasAction()) {
        TypedExtensionConfig action = onMatch.getAction();
        if (!map.containsKey(action.getName())) {
          FilterDelegate delegate = createFilterDelegate(action, context);
          if (delegate != null) {
            map.put(action.getName(), delegate);
          }
        }
      }
    }

    private boolean isSupported() {
      return GrpcUtil.getFlag("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", false);
    }

    private FilterDelegate createFilterDelegate(
        TypedExtensionConfig config, FilterConfigParseContext context) {
      try {
        Any actionAny = config.getTypedConfig();
        if ("type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3.SkipFilter"
            .equals(actionAny.getTypeUrl())) {
          return new FilterDelegate(Collections.emptyList(), null);
        }
        if (!actionAny.is(ExecuteFilterAction.class)) {
          throw new IllegalArgumentException(
              "Expected ExecuteFilterAction or SkipFilter but got: " + actionAny.getTypeUrl());
        }
        ExecuteFilterAction executeAction = actionAny.unpack(ExecuteFilterAction.class);
        FractionalPercent samplePercent = executeAction.hasSamplePercent()
            ? executeAction.getSamplePercent().getDefaultValue()
            : null;
        List<io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig> childConfigs =
            new ArrayList<>();
        if (executeAction.hasFilterChain()) {
          childConfigs.addAll(executeAction.getFilterChain().getTypedConfigList());
        } else if (executeAction.hasTypedConfig()) {
          childConfigs.add(executeAction.getTypedConfig());
        }
        if (childConfigs.isEmpty()) {
          throw new IllegalArgumentException(
              "ExecuteFilterAction must specify either typed_config or a non-empty filter_chain");
        }
        List<DelegateEntry> delegates = new ArrayList<>();
        for (io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig childFilterConfig
            : childConfigs) {
          String typeUrl = childFilterConfig.getTypedConfig().getTypeUrl();
          Message rawConfig = childFilterConfig.getTypedConfig();
          try {
            if ("type.googleapis.com/udpa.type.v1.TypedStruct".equals(typeUrl)) {
              TypedStruct typedStruct =
                  childFilterConfig.getTypedConfig().unpack(TypedStruct.class);
              typeUrl = typedStruct.getTypeUrl();
              rawConfig = typedStruct.getValue();
            } else if ("type.googleapis.com/xds.type.v3.TypedStruct".equals(typeUrl)) {
              com.github.xds.type.v3.TypedStruct typedStruct =
                  childFilterConfig.getTypedConfig().unpack(
                      com.github.xds.type.v3.TypedStruct.class);
              typeUrl = typedStruct.getTypeUrl();
              rawConfig = typedStruct.getValue();
            }
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to unpack TypedStruct", e);
          }
          if (RouterFilter.TYPE_URL.equals(typeUrl)) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter (RouterFilter)");
          }
          Filter.Provider provider = registryLookup.apply(typeUrl);
          if (provider == null) {
            throw new IllegalArgumentException("Action filter not found: " + typeUrl);
          }
          int depth = context.recursionDepth() != null ? context.recursionDepth() : 0;
          if (depth >= 8) {
            throw new IllegalArgumentException("Maximum recursion depth of 8 exceeded");
          }
          Filter.FilterConfigParseContext childContext =
              context.toBuilder().recursionDepth(depth + 1).build();
          ConfigOrError<? extends FilterConfig> parsed =
              provider.parseFilterConfig(rawConfig, childContext);
          if (parsed.errorDetail != null) {
            throw new IllegalArgumentException(
                "Failed to parse child filter: " + parsed.errorDetail);
          }
          if (parsed.config == RouterFilter.ROUTER_CONFIG) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter (RouterFilter)");
          }
          delegates.add(new DelegateEntry(provider, parsed.config, childFilterConfig.getName()));
        }
        return new FilterDelegate(delegates, samplePercent);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static final class CompositeFilterConfig implements FilterConfig {
    @Nullable
    final UnifiedMatcher matcher;
    final Map<String, FilterDelegate> delegates;

    CompositeFilterConfig(@Nullable UnifiedMatcher matcher,
        Map<String, FilterDelegate> delegates) {
      this.matcher = matcher;
      this.delegates = delegates != null
          ? Collections.unmodifiableMap(delegates) : Collections.emptyMap();
    }

    @Override
    public String typeUrl() {
      return TYPE_URL_EXTENSION_WITH_MATCHER;
    }
  }

  static final class FilterDelegate {
    final List<DelegateEntry> delegates;
    private final double threshold;
    private final ThreadSafeRandom random;

    FilterDelegate(List<DelegateEntry> delegates, @Nullable FractionalPercent samplePercent) {
      this(delegates, samplePercent, ThreadSafeRandom.ThreadSafeRandomImpl.instance);
    }

    FilterDelegate(List<DelegateEntry> delegates, @Nullable FractionalPercent samplePercent,
        ThreadSafeRandom random) {
      this.delegates = Collections.unmodifiableList(delegates);
      this.threshold = calculateThreshold(samplePercent);
      this.random = random;
    }

    private static double calculateThreshold(@Nullable FractionalPercent samplePercent) {
      if (samplePercent == null) {
        return 1.0;
      }
      double numerator = samplePercent.getNumerator();
      double denominator;
      switch (samplePercent.getDenominator()) {
        case HUNDRED:
          denominator = 100.0;
          break;
        case TEN_THOUSAND:
          denominator = 10000.0;
          break;
        case MILLION:
          denominator = 1000000.0;
          break;
        default:
          denominator = 100.0;
      }
      return numerator / denominator;
    }

    boolean shouldExecute() {
      if (threshold >= 1.0) {
        return true;
      }
      if (threshold <= 0.0) {
        return false;
      }
      return random.nextDouble() < threshold;
    }
  }

  static final class DelegateEntry {
    final Filter.Provider provider;
    final FilterConfig config;
    final String name;

    DelegateEntry(Filter.Provider provider, FilterConfig config, String name) {
      this.provider = provider;
      this.config = config;
      this.name = name;
    }
  }

  @Override
  public ClientInterceptor buildClientInterceptor(FilterConfig config,
      @Nullable FilterConfig overrideConfig, ScheduledExecutorService scheduler) {
    Preconditions.checkNotNull(config, "config");
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
          Channel next) {
        return new CompositeClientCall<>(
            method, callOptions, next, effective.matcher, effective.delegates, scheduler,
            metricsRecorder);
      }
    };
  }

  @Override
  public ServerInterceptor buildServerInterceptor(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        MatchContext context = MatchContext.newBuilder()
            .setMetadata(headers)
            .setAttributes(call.getAttributes())
            .setMethod(call.getMethodDescriptor().getFullMethodName())
            .setPath("/" + call.getMethodDescriptor().getFullMethodName())
            .setHost(call.getAuthority())
            .build();

        MatchResult matchResult = effective.matcher.match(context);
        if (matchResult == null || !matchResult.matched) {
          call.close(
              Status.UNAVAILABLE.withDescription(
                  "Composite filter: no match found in matcher tree"),
              new Metadata());
          return new ServerCall.Listener<ReqT>() {};
        }

        List<FilterDelegate> matchedDelegates =
            resolveDelegates(matchResult, effective.delegates);

        if (!matchedDelegates.isEmpty()) {
          List<ServerInterceptor> interceptors = new ArrayList<>();
          final List<Filter> filters = new ArrayList<>();
          try {
            for (FilterDelegate delegate : matchedDelegates) {
              if (!delegate.shouldExecute()) {
                continue;
              }
              for (DelegateEntry entry : delegate.delegates) {
                if (!entry.provider.isServerFilter()) {
                  closeAll(filters);
                  call.close(
                      Status.UNAVAILABLE.withDescription(
                          "Filter " + entry.name + " is not supported on server side"),
                      new Metadata());
                  return new ServerCall.Listener<ReqT>() {};
                }
                MetricRecorder recorder =
                    metricsRecorder != null ? metricsRecorder : new MetricRecorder() {};
                Filter filter = entry.provider.newInstance(
                    FilterContext.create(entry.name, recorder));
                filters.add(filter);
                ServerInterceptor interceptor = filter.buildServerInterceptor(entry.config, null);
                if (interceptor != null) {
                  interceptors.add(interceptor);
                }
              }
            }
          } catch (Throwable t) {
            closeAll(filters);
            throw t;
          }

          if (!interceptors.isEmpty()) {
            ServerCallHandler<ReqT, RespT> wrapped = next;
            for (int i = interceptors.size() - 1; i >= 0; i--) {
              final ServerInterceptor interceptor = interceptors.get(i);
              final ServerCallHandler<ReqT, RespT> current = wrapped;
              wrapped = new ServerCallHandler<ReqT, RespT>() {
                @Override
                public ServerCall.Listener<ReqT> startCall(
                    ServerCall<ReqT, RespT> call, Metadata headers) {
                  return interceptor.interceptCall(call, headers, current);
                }
              };
            }
            final AtomicBoolean closed = new AtomicBoolean();
            final Runnable doClose = () -> {
              if (closed.compareAndSet(false, true)) {
                closeAll(filters);
              }
            };
            ServerCall.Listener<ReqT> listener;
            try {
              listener = wrapped.startCall(call, headers);
            } catch (Throwable t) {
              doClose.run();
              throw t;
            }
            return new SimpleForwardingServerCallListener<ReqT>(listener) {
              @Override
              public void onCancel() {
                try {
                  super.onCancel();
                } finally {
                  doClose.run();
                }
              }

              @Override
              public void onComplete() {
                try {
                  super.onComplete();
                } finally {
                  doClose.run();
                }
              }
            };
          } else {
            closeAll(filters);
          }
        }
        return next.startCall(call, headers);
      }
    };
  }

  private static CompositeFilterConfig getEffectiveConfig(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    CompositeFilterConfig effective = (CompositeFilterConfig) config;
    if (overrideConfig != null) {
      CompositeFilterConfig override = (CompositeFilterConfig) overrideConfig;
      if (override.matcher != null) {
        return override;
      }
    }
    return effective;
  }

  static List<FilterDelegate> resolveDelegates(@Nullable MatchResult matchResult,
      Map<String, FilterDelegate> delegatesMap) {
    if (matchResult == null || !matchResult.matched || matchResult.actions.isEmpty()) {
      return Collections.emptyList();
    }
    List<FilterDelegate> list = new ArrayList<>();
    for (TypedExtensionConfig action : matchResult.actions) {
      FilterDelegate d = delegatesMap.get(action.getName());
      if (d != null) {
        list.add(d);
      }
    }
    return Collections.unmodifiableList(list);
  }

  private static void closeAll(Iterable<Filter> filters) {
    Throwable firstException = null;
    for (Filter f : filters) {
      try {
        f.close();
      } catch (Throwable t) {
        if (firstException == null) {
          firstException = t;
        } else {
          firstException.addSuppressed(t);
        }
      }
    }
    if (firstException instanceof RuntimeException) {
      throw (RuntimeException) firstException;
    } else if (firstException instanceof Error) {
      throw (Error) firstException;
    }
  }

  private static final class CompositeClientCall<ReqT, RespT>
      extends ForwardingClientCall<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> method;
    private final CallOptions callOptions;
    private final Channel next;
    private final UnifiedMatcher matcher;
    private final Map<String, FilterDelegate> delegatesMap;
    private final ScheduledExecutorService scheduler;
    @Nullable
    private final MetricRecorder metricsRecorder;
    private final Object lock = new Object();
    private ClientCall<ReqT, RespT> delegate;
    private boolean started;
    private Status cancelStatus;

    CompositeClientCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
        Channel next, UnifiedMatcher matcher, Map<String, FilterDelegate> delegatesMap,
        ScheduledExecutorService scheduler, @Nullable MetricRecorder metricsRecorder) {
      this.method = method;
      this.callOptions = callOptions;
      this.next = next;
      this.matcher = matcher;
      this.delegatesMap = delegatesMap;
      this.scheduler = scheduler;
      this.metricsRecorder = metricsRecorder;
    }

    private static final ClientCall<Object, Object> NOOP_CALL =
        new ClientCall<Object, Object>() {
          @Override
          public void start(Listener<Object> responseListener, Metadata headers) {}

          @Override
          public void request(int numMessages) {}

          @Override
          public void cancel(@Nullable String message, @Nullable Throwable cause) {}

          @Override
          public void halfClose() {}

          @Override
          public void sendMessage(Object message) {}
        };

    @SuppressWarnings("unchecked")
    private static <ReqT, RespT> ClientCall<ReqT, RespT> noopCall() {
      return (ClientCall<ReqT, RespT>) NOOP_CALL;
    }

    @Override
    protected ClientCall<ReqT, RespT> delegate() {
      synchronized (lock) {
        Preconditions.checkState(started, "Not started");
        return delegate != null ? delegate : noopCall();
      }
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      ClientCall<ReqT, RespT> callToCancel = null;
      synchronized (lock) {
        if (cancelStatus == null) {
          cancelStatus = Status.CANCELLED.withDescription(message).withCause(cause);
        }
        if (delegate != null) {
          callToCancel = delegate;
        }
      }
      if (callToCancel != null) {
        callToCancel.cancel(message, cause);
      }
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      synchronized (lock) {
        Preconditions.checkState(!started, "Already started");
        started = true;

        if (cancelStatus != null) {
          delegate = noopCall();
          responseListener.onClose(cancelStatus, new Metadata());
          return;
        }
      }

      String host = callOptions.getAuthority() != null
          ? callOptions.getAuthority() : next.authority();
      MatchContext context = MatchContext.newBuilder()
          .setMetadata(headers)
          .setAttributes(Attributes.EMPTY)
          .setCallOptions(callOptions)
          .setMethod(method.getFullMethodName())
          .setPath("/" + method.getFullMethodName())
          .setHost(host)
          .build();

      MatchResult matchResult = matcher.match(context);
      if (matchResult == null || !matchResult.matched) {
        synchronized (lock) {
          delegate = noopCall();
        }
        responseListener.onClose(
            Status.UNAVAILABLE.withDescription("Composite filter: no match found in matcher tree"),
            new Metadata());
        return;
      }

      final List<Filter> filters = new ArrayList<>();
      ClientCall<ReqT, RespT> realCall = null;
      try {
        List<FilterDelegate> filterDelegates = resolveDelegates(matchResult, delegatesMap);
        if (!filterDelegates.isEmpty()) {
          List<ClientInterceptor> interceptors = new ArrayList<>();
          for (FilterDelegate filterDelegate : filterDelegates) {
            if (!filterDelegate.shouldExecute()) {
              continue;
            }
            for (DelegateEntry entry : filterDelegate.delegates) {
              if (!entry.provider.isClientFilter()) {
                closeAll(filters);
                synchronized (lock) {
                  delegate = noopCall();
                }
                responseListener.onClose(
                    Status.UNAVAILABLE.withDescription(
                        "Filter " + entry.name + " is not supported on client side"),
                    new Metadata());
                return;
              }
              MetricRecorder recorder =
                  metricsRecorder != null ? metricsRecorder : new MetricRecorder() {};
              Filter filter = entry.provider.newInstance(
                  FilterContext.create(entry.name, recorder));
              filters.add(filter);
              ClientInterceptor interceptor =
                  filter.buildClientInterceptor(entry.config, null, scheduler);
              if (interceptor != null) {
                interceptors.add(interceptor);
              }
            }
          }

          if (!interceptors.isEmpty()) {
            realCall =
                ClientInterceptors.intercept(next, interceptors).newCall(method, callOptions);
            responseListener = new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onClose(Status status, Metadata trailers) {
                try {
                  super.onClose(status, trailers);
                } finally {
                  closeAll(filters);
                }
              }
            };
          } else {
            closeAll(filters);
          }
        }

        if (realCall == null) {
          realCall = next.newCall(method, callOptions);
        }

        synchronized (lock) {
          if (cancelStatus != null) {
            closeAll(filters);
            delegate = noopCall();
            responseListener.onClose(cancelStatus, new Metadata());
            return;
          }
          delegate = realCall;
        }
        realCall.start(responseListener, headers);
      } catch (Throwable t) {
        closeAll(filters);
        throw t;
      }
    }
  }
}
