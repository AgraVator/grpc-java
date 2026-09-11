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
import io.grpc.InternalServerInterceptors;
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
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

final class CompositeFilter implements Filter {

  static final String TYPE_URL_EXTENSION_WITH_MATCHER =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcher";
  static final String TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute";

  @Nullable
  private final MetricRecorder metricsRecorder;

  private final Object filtersLock = new Object();

  /**
   * Nested filter instances claimed by the current configuration generation, keyed by nested
   * filter name and type URL, mirroring {@link Filter.NamedFilterConfig#filterStateKey}.
   */
  @GuardedBy("filtersLock")
  private final Map<String, Filter> activeNestedFilters = new HashMap<>();

  /**
   * Nested filters held over from the previous configuration generation. A filter that is still
   * configured is promoted back into {@link #activeNestedFilters} on first use, preserving the
   * state it owns; whatever remains unclaimed is closed when the next generation begins.
   */
  @GuardedBy("filtersLock")
  private final Map<String, Filter> retiredNestedFilters = new HashMap<>();

  /**
   * The top-level {@link FilterConfig} that defines the current generation, compared by identity.
   * A given LDS update produces exactly one config object, which the resolver then passes to
   * every route, so a change of identity marks a new generation.
   */
  @GuardedBy("filtersLock")
  @Nullable
  private FilterConfig currentGeneration;

  @GuardedBy("filtersLock")
  private boolean closed;

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
        // A missing xds_matcher is permitted here and makes the filter a no-op passthrough; a
        // per-route override may still supply one. This matches gRPC C++, whose
        // ParseTopLevelConfig() leaves config->matcher null without adding a validation error,
        // and whose data plane then starts the child call directly. The deprecated `matcher`
        // field is ignored, per A103.
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
        // Unlike the top-level config, a per-route override carries nothing but the matcher, so
        // an absent one is a configuration error rather than a no-op. gRPC C++ likewise NACKs
        // here, with "...ExtensionWithMatcherPerRoute].xds_matcher error:field not set".
        if (!proto.hasXdsMatcher()) {
          return ConfigOrError.fromError(
              "ExtensionWithMatcherPerRoute.xds_matcher: field not set");
        }
        return parseMatcherConfig(proto.getXdsMatcher(), context);
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

  /**
   * A {@link FilterDelegate} whose nested filter interceptors have already been built, at
   * configuration time.
   *
   * <p>Per RPC we only need to evaluate {@link FilterDelegate#shouldExecute} and reuse
   * {@link #interceptors}. If a nested filter is not supported on the side being built,
   * {@link #error} holds the status to fail matching RPCs with; the error is deliberately
   * deferred to RPC time so that a delegate which is never sampled in never fails anything.
   */
  static final class ResolvedDelegate<I> {
    final FilterDelegate delegate;
    final List<I> interceptors;
    @Nullable
    final Status error;

    private ResolvedDelegate(FilterDelegate delegate, List<I> interceptors,
        @Nullable Status error) {
      this.delegate = delegate;
      this.interceptors = Collections.unmodifiableList(interceptors);
      this.error = error;
    }

    static <I> ResolvedDelegate<I> of(FilterDelegate delegate, List<I> interceptors) {
      return new ResolvedDelegate<>(delegate, interceptors, /* error= */ null);
    }

    static <I> ResolvedDelegate<I> error(FilterDelegate delegate, Status error) {
      return new ResolvedDelegate<>(delegate, Collections.<I>emptyList(), error);
    }
  }

  @Override
  public ClientInterceptor buildClientInterceptor(FilterConfig config,
      @Nullable FilterConfig overrideConfig, ScheduledExecutorService scheduler) {
    Preconditions.checkNotNull(config, "config");
    // Done before the early return below, so that a configuration which no longer has a matcher
    // still releases the nested filters of the generation it replaced.
    closeAll(rotateGenerationIfNeeded(config));
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    final Map<String, ResolvedDelegate<ClientInterceptor>> resolvedDelegates =
        resolveAll(effective, delegate -> resolveClientDelegate(delegate, scheduler));

    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
          Channel next) {
        return new CompositeClientCall<>(
            method, callOptions, next, effective.matcher, resolvedDelegates);
      }
    };
  }

  @Override
  public ServerInterceptor buildServerInterceptor(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    Preconditions.checkNotNull(config, "config");
    closeAll(rotateGenerationIfNeeded(config));
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    final Map<String, ResolvedDelegate<ServerInterceptor>> resolvedDelegates =
        resolveAll(effective, this::resolveServerDelegate);

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

        // Only matcher evaluation and sampling happen per RPC. The nested filters and their
        // interceptors were already built once above, at configuration time.
        List<ServerInterceptor> interceptors = new ArrayList<>();
        for (ResolvedDelegate<ServerInterceptor> resolved
            : resolveDelegates(matchResult, resolvedDelegates)) {
          if (!resolved.delegate.shouldExecute()) {
            continue;
          }
          if (resolved.error != null) {
            call.close(resolved.error, new Metadata());
            return new ServerCall.Listener<ReqT>() {};
          }
          interceptors.addAll(resolved.interceptors);
        }

        ServerCallHandler<ReqT, RespT> wrapped = next;
        for (int i = interceptors.size() - 1; i >= 0; i--) {
          wrapped = InternalServerInterceptors.interceptCallHandlerCreate(
              interceptors.get(i), wrapped);
        }
        return wrapped.startCall(call, headers);
      }
    };
  }

  /**
   * Builds the nested filters and their interceptors for every action in the matcher tree. This
   * runs once per configuration update, not per RPC.
   *
   * <p>This mirrors the reference implementations. In Envoy, {@code ExecuteFilterAction} stores an
   * {@code Http::FilterFactoryCb} created by {@code ExecuteFilterActionFactory::createAction()} at
   * config load and merely invokes it per stream. In gRPC C++, {@code CompositeFilter} caches
   * per-action filter chains in {@code filter_chain_map_}. Creating nested filters per RPC would
   * defeat the caches and shared connections that {@link Filter} implementations are documented to
   * own, and would violate {@link Filter.Provider#newInstance}'s lifecycle contract.
   */
  private <I> Map<String, ResolvedDelegate<I>> resolveAll(
      CompositeFilterConfig effective, Function<FilterDelegate, ResolvedDelegate<I>> resolver) {
    Map<String, ResolvedDelegate<I>> resolved = new HashMap<>();
    for (Map.Entry<String, FilterDelegate> entry : effective.delegates.entrySet()) {
      resolved.put(entry.getKey(), resolver.apply(entry.getValue()));
    }
    return Collections.unmodifiableMap(resolved);
  }

  private ResolvedDelegate<ServerInterceptor> resolveServerDelegate(FilterDelegate delegate) {
    List<ServerInterceptor> interceptors = new ArrayList<>();
    for (DelegateEntry entry : delegate.delegates) {
      if (!entry.provider.isServerFilter()) {
        return ResolvedDelegate.error(delegate,
            Status.UNAVAILABLE.withDescription(
                "Filter " + entry.name + " is not supported on server side"));
      }
      ServerInterceptor interceptor = getOrCreateNestedFilter(entry)
          .buildServerInterceptor(entry.config, /* overrideConfig= */ null);
      if (interceptor != null) {
        interceptors.add(interceptor);
      }
    }
    return ResolvedDelegate.of(delegate, interceptors);
  }

  private ResolvedDelegate<ClientInterceptor> resolveClientDelegate(
      FilterDelegate delegate, ScheduledExecutorService scheduler) {
    List<ClientInterceptor> interceptors = new ArrayList<>();
    for (DelegateEntry entry : delegate.delegates) {
      if (!entry.provider.isClientFilter()) {
        return ResolvedDelegate.error(delegate,
            Status.UNAVAILABLE.withDescription(
                "Filter " + entry.name + " is not supported on client side"));
      }
      ClientInterceptor interceptor = getOrCreateNestedFilter(entry)
          .buildClientInterceptor(entry.config, /* overrideConfig= */ null, scheduler);
      if (interceptor != null) {
        interceptors.add(interceptor);
      }
    }
    return ResolvedDelegate.of(delegate, interceptors);
  }

  /**
   * Rotates the nested filter generations if {@code topLevelConfig} is not the one that defined
   * the current generation, and returns the filters that are now provably unused.
   *
   * <p>Nested filters must outlive a configuration update, because the state they own - a
   * connection pool, a credential cache - is exactly what would be lost by rebuilding them. So a
   * new generation retires the previous one wholesale, {@link #getOrCreateNestedFilter} promotes
   * back anything still configured, and only the leftovers are closed.
   *
   * <p>gRPC C++ solves the same problem with its {@code Blackboard}. It has used two designs. The
   * original one is what this method implements: an explicit carry-forward step
   * ({@code UpdateBlackboard(old_blackboard, new_blackboard)}) followed by dropping the old
   * container, so unclaimed state died deterministically at the swap. C++ has since replaced it
   * with a single long-lived blackboard holding weak references, letting refcounting destroy an
   * entry once the last config referencing it goes away. That second design does not port to
   * Java: {@link Filter#close} has to be called explicitly, and garbage collection will not do it,
   * so the resources would leak even after the object became unreachable. Hence the explicit
   * approach.
   *
   * <p>One difference from C++: a configuration generation here has no explicit end, since the
   * resolver calls {@code buildXInterceptor} once per route and never signals the last one. A
   * generation's leftovers can therefore only be released once the following generation begins,
   * which bounds live instances at two generations instead of letting them grow without limit.
   */
  private List<Filter> rotateGenerationIfNeeded(FilterConfig topLevelConfig) {
    synchronized (filtersLock) {
      // Identity, not equality: one LDS update yields one config object, shared by every route.
      if (currentGeneration == topLevelConfig) {
        return new ArrayList<>();
      }
      currentGeneration = topLevelConfig;
      List<Filter> unclaimed = new ArrayList<>(retiredNestedFilters.values());
      retiredNestedFilters.clear();
      retiredNestedFilters.putAll(activeNestedFilters);
      activeNestedFilters.clear();
      return unclaimed;
    }
  }

  /**
   * Returns the nested {@link Filter} for {@code entry}, promoting it from the previous
   * generation when possible and only creating a new instance as a last resort.
   */
  private Filter getOrCreateNestedFilter(DelegateEntry entry) {
    String key = entry.name + "_" + entry.config.typeUrl();
    synchronized (filtersLock) {
      Preconditions.checkState(!closed, "CompositeFilter is closed");
      Filter filter = activeNestedFilters.get(key);
      if (filter == null) {
        // Still configured, so carry the instance - and the state it owns - across the update.
        filter = retiredNestedFilters.remove(key);
      }
      if (filter == null) {
        MetricRecorder recorder =
            metricsRecorder != null ? metricsRecorder : new MetricRecorder() {};
        filter = entry.provider.newInstance(FilterContext.create(entry.name, recorder));
      }
      activeNestedFilters.put(key, filter);
      return filter;
    }
  }

  @Override
  public void close() {
    List<Filter> toClose;
    synchronized (filtersLock) {
      if (closed) {
        return;
      }
      closed = true;
      toClose = new ArrayList<>(activeNestedFilters.size() + retiredNestedFilters.size());
      toClose.addAll(activeNestedFilters.values());
      toClose.addAll(retiredNestedFilters.values());
      activeNestedFilters.clear();
      retiredNestedFilters.clear();
      currentGeneration = null;
    }
    closeAll(toClose);
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

  static <I> List<ResolvedDelegate<I>> resolveDelegates(@Nullable MatchResult matchResult,
      Map<String, ResolvedDelegate<I>> delegatesMap) {
    if (matchResult == null || !matchResult.matched || matchResult.actions.isEmpty()) {
      return Collections.emptyList();
    }
    List<ResolvedDelegate<I>> list = new ArrayList<>();
    for (TypedExtensionConfig action : matchResult.actions) {
      ResolvedDelegate<I> d = delegatesMap.get(action.getName());
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
    private final Map<String, ResolvedDelegate<ClientInterceptor>> delegatesMap;
    private final Object lock = new Object();
    private ClientCall<ReqT, RespT> delegate;
    private boolean started;
    private Status cancelStatus;

    CompositeClientCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
        Channel next, UnifiedMatcher matcher,
        Map<String, ResolvedDelegate<ClientInterceptor>> delegatesMap) {
      this.method = method;
      this.callOptions = callOptions;
      this.next = next;
      this.matcher = matcher;
      this.delegatesMap = delegatesMap;
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
        failCall(responseListener,
            Status.UNAVAILABLE.withDescription("Composite filter: no match found in matcher tree"));
        return;
      }

      // Only matcher evaluation and sampling happen per RPC. The nested filters and their
      // interceptors were already built once at configuration time; see
      // CompositeFilter#buildClientInterceptor.
      List<ClientInterceptor> interceptors = new ArrayList<>();
      for (ResolvedDelegate<ClientInterceptor> resolved
          : resolveDelegates(matchResult, delegatesMap)) {
        if (!resolved.delegate.shouldExecute()) {
          continue;
        }
        if (resolved.error != null) {
          failCall(responseListener, resolved.error);
          return;
        }
        interceptors.addAll(resolved.interceptors);
      }

      ClientCall<ReqT, RespT> realCall = interceptors.isEmpty()
          ? next.newCall(method, callOptions)
          : ClientInterceptors.intercept(next, interceptors).newCall(method, callOptions);

      synchronized (lock) {
        if (cancelStatus != null) {
          delegate = noopCall();
          responseListener.onClose(cancelStatus, new Metadata());
          return;
        }
        delegate = realCall;
      }
      realCall.start(responseListener, headers);
    }

    private void failCall(Listener<RespT> responseListener, Status status) {
      synchronized (lock) {
        delegate = noopCall();
      }
      responseListener.onClose(status, new Metadata());
    }
  }
}
