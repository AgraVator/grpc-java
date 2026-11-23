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
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.common.matcher.action.v3.SkipFilter;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.InternalServerInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.internal.GrpcUtil;
import io.grpc.xds.internal.matcher.MatchContext;
import io.grpc.xds.internal.matcher.MatchResult;
import io.grpc.xds.internal.matcher.UnifiedMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Composite HTTP filter implementation per gRFC A103.
 *
 * <p>Holds no per-instance state of its own: nested filter instances live in the resolver's or
 * server's active-filter map keyed by {@link NamedFilterConfig#filterStateKey()}, declared via
 * {@link FilterConfig#nestedFilterConfigs()} and resolved via {@link FilterContext#filterLookup()}.
 */
final class CompositeFilter implements Filter {

  static final String TYPE_URL_EXTENSION_WITH_MATCHER =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcher";
  static final String TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute";
  static final String TYPE_URL_COMPOSITE =
      "type.googleapis.com/envoy.extensions.filters.http.composite.v3.Composite";
  private static final String TYPE_URL_TYPED_STRUCT_UDPA =
      "type.googleapis.com/udpa.type.v1.TypedStruct";
  private static final String TYPE_URL_TYPED_STRUCT_XDS =
      "type.googleapis.com/xds.type.v3.TypedStruct";
  private static final Logger logger = Logger.getLogger(CompositeFilter.class.getName());
  private static final FilterDelegate SKIP_DELEGATE =
      new FilterDelegate(Collections.emptyList(), null);

  @Nullable
  private final Function<NamedFilterConfig, Filter> filterLookup;

  CompositeFilter(@Nullable Function<NamedFilterConfig, Filter> filterLookup) {
    this.filterLookup = filterLookup;
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
      return isSupported();
    }

    @Override
    public boolean isServerFilter() {
      return isSupported();
    }

    @Override
    public Filter newInstance(FilterContext context) {
      return new CompositeFilter(context.filterLookup());
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfig(
        Message rawProtoMessage, FilterConfigParseContext context) {
      ConfigOrError<ExtensionWithMatcher> unpacked =
          unpackConfigProto(rawProtoMessage, context, ExtensionWithMatcher.class);
      if (unpacked.errorDetail != null) {
        return ConfigOrError.fromError(unpacked.errorDetail);
      }
      ExtensionWithMatcher proto = unpacked.config;
      try {
        if (!proto.hasExtensionConfig()
            || !proto.getExtensionConfig().hasTypedConfig()
            || !TYPE_URL_COMPOSITE.equals(
                unwrapTypedStructTypeUrl(proto.getExtensionConfig().getTypedConfig()))) {
          return ConfigOrError.fromError(
              "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
        }
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }
      // Missing xds_matcher is a no-op passthrough; deprecated `matcher` is ignored per A103.
      return parseMatcherConfig(proto.hasXdsMatcher() ? proto.getXdsMatcher() : null, context);
    }

    private static String unwrapTypedStructTypeUrl(Any any) throws InvalidProtocolBufferException {
      String typeUrl = any.getTypeUrl();
      if (TYPE_URL_TYPED_STRUCT_UDPA.equals(typeUrl)) {
        return any.unpack(TypedStruct.class).getTypeUrl();
      }
      if (TYPE_URL_TYPED_STRUCT_XDS.equals(typeUrl)) {
        return any.unpack(com.github.xds.type.v3.TypedStruct.class).getTypeUrl();
      }
      return typeUrl;
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfigOverride(
        Message rawProtoMessage, FilterConfigParseContext context) {
      ConfigOrError<ExtensionWithMatcherPerRoute> unpacked =
          unpackConfigProto(rawProtoMessage, context, ExtensionWithMatcherPerRoute.class);
      if (unpacked.errorDetail != null) {
        return ConfigOrError.fromError(unpacked.errorDetail);
      }
      if (!unpacked.config.hasXdsMatcher()) {
        return ConfigOrError.fromError("ExtensionWithMatcherPerRoute.xds_matcher must be set");
      }
      return parseMatcherConfig(unpacked.config.getXdsMatcher(), context);
    }

    private static <T extends Message> ConfigOrError<T> unpackConfigProto(
        Message rawProtoMessage, FilterConfigParseContext context, Class<T> clazz) {
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError(
            "Invalid message type: " + rawProtoMessage.getClass().getName());
      }
      if (context.recursionDepth() >= FilterConfigParseContext.MAX_RECURSION_DEPTH) {
        return ConfigOrError.fromError("Maximum recursion depth of "
            + FilterConfigParseContext.MAX_RECURSION_DEPTH + " exceeded");
      }
      Any any = (Any) rawProtoMessage;
      if (!any.is(clazz)) {
        return ConfigOrError.fromError(
            "Expected " + clazz.getSimpleName() + " but got: " + any.getTypeUrl());
      }
      try {
        return ConfigOrError.fromConfig(any.unpack(clazz));
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }
    }

    private ConfigOrError<CompositeFilterConfig> parseMatcherConfig(
        @Nullable Matcher matcherProto, FilterConfigParseContext context) {
      if (matcherProto == null) {
        return ConfigOrError.fromConfig(
            new CompositeFilterConfig(null, Collections.emptyMap(), /* matcherProto= */ null));
      }
      try {
        Map<TypedExtensionConfig, FilterDelegate> delegates = new LinkedHashMap<>();
        collectDelegates(matcherProto, delegates, context, /* matcherDepth= */ 0);
        UnifiedMatcher matcher = UnifiedMatcher.fromProto(
            matcherProto, this::validateActionTypeUrl);
        return ConfigOrError.fromConfig(
            new CompositeFilterConfig(matcher, delegates, matcherProto));
      } catch (Exception e) {
        return ConfigOrError.fromError("Failed to create matcher: " + e.getMessage());
      }
    }

    private boolean validateActionTypeUrl(String typeUrl) {
      return "type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3.SkipFilter"
          .equals(typeUrl)
          || "type.googleapis.com/envoy.extensions.filters.http.composite.v3.ExecuteFilterAction"
          .equals(typeUrl);
    }

    /**
     * Walks {@code matcher} and every matcher nested in its on_match branches, parsing each
     * action into {@code map}. A106 requires every Matcher to have a matcher_list or a
     * matcher_tree; one with neither (an empty one, or one with only on_no_match) is rejected,
     * as C-core does, rather than accepted as an always-on_no_match matcher.
     */
    private void collectDelegates(Matcher matcher, Map<TypedExtensionConfig, FilterDelegate> map,
        FilterConfigParseContext context, int matcherDepth) {
      if (matcherDepth > UnifiedMatcher.MAX_RECURSION_DEPTH) {
        throw new IllegalArgumentException(
            "Matcher tree depth exceeds limit of " + UnifiedMatcher.MAX_RECURSION_DEPTH);
      }
      if (!matcher.hasMatcherList() && !matcher.hasMatcherTree()) {
        throw new IllegalArgumentException(
            "xds_matcher: no matcher_list or matcher_tree specified");
      }
      if (matcher.hasMatcherList()) {
        for (Matcher.MatcherList.FieldMatcher fm : matcher.getMatcherList().getMatchersList()) {
          if (fm.hasOnMatch()) {
            collectOnMatch(fm.getOnMatch(), map, context, matcherDepth);
          }
        }
      } else if (matcher.hasMatcherTree()) {
        Matcher.MatcherTree tree = matcher.getMatcherTree();
        if (tree.hasExactMatchMap()) {
          for (Matcher.OnMatch om : tree.getExactMatchMap().getMapMap().values()) {
            collectOnMatch(om, map, context, matcherDepth);
          }
        } else if (tree.hasPrefixMatchMap()) {
          for (Matcher.OnMatch om : tree.getPrefixMatchMap().getMapMap().values()) {
            collectOnMatch(om, map, context, matcherDepth);
          }
        }
      }
      if (matcher.hasOnNoMatch()) {
        collectOnMatch(matcher.getOnNoMatch(), map, context, matcherDepth);
      }
    }

    private void collectOnMatch(Matcher.OnMatch onMatch,
        Map<TypedExtensionConfig, FilterDelegate> map,
        FilterConfigParseContext context,
        int matcherDepth) {
      if (onMatch.getKeepMatching()) {
        throw new IllegalArgumentException(
            "keep_matching is not permitted anywhere in the composite filter matcher tree");
      }
      if (onMatch.hasMatcher()) {
        collectDelegates(onMatch.getMatcher(), map, context, matcherDepth + 1);
      } else if (onMatch.hasAction()) {
        TypedExtensionConfig action = onMatch.getAction();
        // Keyed by the action message rather than by action.name: `name` is an opaque identifier
        // that "is not used to select the extension", and nothing constrains it to be unique
        // across actions, so two distinct actions may legitimately share one. Identical actions
        // collapse to a single entry, which is intended: the same action reached from several
        // matcher branches is one delegate, parsed once.
        if (!map.containsKey(action)) {
          map.put(action, createFilterDelegate(action, context));
        }
      }
    }

    // Both side predicates delegate here rather than parseFilterConfig checking the flag, so that
    // a disabled composite filter looks like any other filter this build does not support. That
    // distinction matters: XdsListenerResource honours the http_filter's is_optional flag when a
    // filter is unsupported, but treats a parse error as fatal, so checking the flag while
    // parsing would NACK the whole listener over an optional filter.
    private boolean isSupported() {
      return GrpcUtil.getFlag("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", false);
    }

    private FilterDelegate createFilterDelegate(TypedExtensionConfig config,
        FilterConfigParseContext context) {
      try {
        Any actionAny = config.getTypedConfig();
        if (actionAny.is(SkipFilter.class)) {
          try {
            SkipFilter unused = actionAny.unpack(SkipFilter.class);
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Could not parse SkipFilter action", e);
          }
          return SKIP_DELEGATE;
        }
        if (!actionAny.is(ExecuteFilterAction.class)) {
          throw new IllegalArgumentException(
              "Expected ExecuteFilterAction or SkipFilter but got: " + actionAny.getTypeUrl());
        }
        ExecuteFilterAction executeAction = actionAny.unpack(ExecuteFilterAction.class);
        FractionalPercent samplePercent = null;
        if (executeAction.hasSamplePercent()) {
          if (!executeAction.getSamplePercent().hasDefaultValue()) {
            throw new IllegalArgumentException(
                "ExecuteFilterAction.sample_percent.default_value: field not set");
          }
          samplePercent = executeAction.getSamplePercent().getDefaultValue();
        }
        List<io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig> childConfigs;
        if (executeAction.hasFilterChain()) {
          childConfigs = executeAction.getFilterChain().getTypedConfigList();
        } else if (executeAction.hasTypedConfig()) {
          childConfigs = Collections.singletonList(executeAction.getTypedConfig());
        } else {
          throw new IllegalArgumentException(
              "ExecuteFilterAction must specify either typed_config or filter_chain");
        }
        List<NestedFilter> delegates = new ArrayList<>(childConfigs.size());
        for (io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig childFilterConfig
            : childConfigs) {
          Any childAny = childFilterConfig.getTypedConfig();
          String typeUrl = childAny.getTypeUrl();
          Message rawConfig = childAny;
          try {
            if (TYPE_URL_TYPED_STRUCT_UDPA.equals(typeUrl)) {
              TypedStruct typedStruct = childAny.unpack(TypedStruct.class);
              typeUrl = typedStruct.getTypeUrl();
              rawConfig = typedStruct.getValue();
            } else if (TYPE_URL_TYPED_STRUCT_XDS.equals(typeUrl)) {
              com.github.xds.type.v3.TypedStruct typedStruct =
                  childAny.unpack(com.github.xds.type.v3.TypedStruct.class);
              typeUrl = typedStruct.getTypeUrl();
              rawConfig = typedStruct.getValue();
            }
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to unpack TypedStruct", e);
          }
          if (RouterFilter.TYPE_URL.equals(typeUrl)) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter: " + typeUrl);
          }
          Filter.Provider provider = registryLookup.apply(typeUrl);
          if (provider == null || (!provider.isClientFilter() && !provider.isServerFilter())) {
            throw new IllegalArgumentException("Action filter not found: " + typeUrl);
          }

          Filter.FilterConfigParseContext childContext =
              context.toBuilder().recursionDepth(context.recursionDepth() + 1).build();
          ConfigOrError<? extends FilterConfig> parsed =
              provider.parseFilterConfig(rawConfig, childContext);
          if (parsed.errorDetail != null) {
            throw new IllegalArgumentException(
                "Failed to parse child filter: " + parsed.errorDetail);
          }
          if (parsed.config == RouterFilter.ROUTER_CONFIG) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter: " + typeUrl);
          }
          delegates.add(new NestedFilter(
              new NamedFilterConfig(childFilterConfig.getName(), parsed.config), provider));
        }
        return new FilterDelegate(delegates, samplePercent);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Could not parse ExecuteFilterAction", e);
      }
    }
  }

  static final class CompositeFilterConfig implements FilterConfig {
    @Nullable
    final UnifiedMatcher matcher;
    final Map<TypedExtensionConfig, FilterDelegate> delegates;
    /**
     * The proto the other two fields were parsed from, retained solely to give this config value
     * equality. Neither {@link UnifiedMatcher} nor {@link FilterDelegate} implements
     * {@code equals}, but parsing is deterministic, so two configs built from equal protos are
     * interchangeable.
     */
    @Nullable
    private final Matcher matcherProto;

    @VisibleForTesting
    CompositeFilterConfig(@Nullable UnifiedMatcher matcher,
        Map<TypedExtensionConfig, FilterDelegate> delegates) {
      this(matcher, delegates, /* matcherProto= */ null);
    }

    CompositeFilterConfig(@Nullable UnifiedMatcher matcher,
        Map<TypedExtensionConfig, FilterDelegate> delegates, @Nullable Matcher matcherProto) {
      this.matcher = matcher;
      this.delegates = delegates != null
          ? Collections.unmodifiableMap(delegates) : Collections.emptyMap();
      this.matcherProto = matcherProto;
    }

    @Override
    public String typeUrl() {
      return TYPE_URL_EXTENSION_WITH_MATCHER;
    }

    /** Every nested filter config in this composite filter tree, recursively. */
    @Override
    public List<NamedFilterConfig> nestedFilterConfigs() {
      List<NamedFilterConfig> nested = new ArrayList<>();
      for (FilterDelegate delegate : delegates.values()) {
        for (NestedFilter nestedFilter : delegate.delegates) {
          nested.add(nestedFilter.config);
          nested.addAll(nestedFilter.config.filterConfig.nestedFilterConfigs());
        }
      }
      return nested;
    }

    /**
     * Value equality, so that a control plane re-sending an unchanged resource does not look like
     * a configuration change. The xDS client compares parsed resources to decide whether to notify
     * watchers, and identity semantics here would wake every watcher on every LDS refresh.
     */
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CompositeFilterConfig)) {
        return false;
      }
      CompositeFilterConfig that = (CompositeFilterConfig) o;
      if (matcherProto != null || that.matcherProto != null) {
        return Objects.equals(matcherProto, that.matcherProto);
      }
      // Neither was parsed from a proto: either both are the no-matcher passthrough, whose
      // matcher is null, or both were hand-built. Fall back to matcher identity.
      return matcher == that.matcher;
    }

    @Override
    public int hashCode() {
      return matcherProto != null ? matcherProto.hashCode() : System.identityHashCode(matcher);
    }

    @Override
    public String toString() {
      return "CompositeFilterConfig{matcher=" + (matcher == null ? "none" : "set")
          + ", delegates=" + delegates.size() + "}";
    }
  }

  static final class FilterDelegate {
    final List<NestedFilter> delegates;
    private final int ratePerMillion;
    private final ThreadSafeRandom random;

    FilterDelegate(List<NestedFilter> delegates, @Nullable FractionalPercent samplePercent) {
      this(delegates, samplePercent, ThreadSafeRandom.ThreadSafeRandomImpl.instance);
    }

    FilterDelegate(List<NestedFilter> delegates, @Nullable FractionalPercent samplePercent,
        ThreadSafeRandom random) {
      this.delegates = Collections.unmodifiableList(delegates);
      this.ratePerMillion = calculateRatePerMillion(samplePercent);
      this.random = random;
    }

    private static int calculateRatePerMillion(@Nullable FractionalPercent samplePercent) {
      if (samplePercent == null) {
        return 1_000_000;
      }
      long numerator = Integer.toUnsignedLong(samplePercent.getNumerator());
      long rate;
      switch (samplePercent.getDenominator()) {
        case HUNDRED:
          rate = numerator * 10_000L;
          break;
        case TEN_THOUSAND:
          rate = numerator * 100L;
          break;
        case MILLION:
          rate = numerator;
          break;
        case UNRECOGNIZED:
        default:
          // Guessing here would silently widen the sample: a numerator meant as a fraction of a
          // denominator we do not know would be read as a percentage. Reject the config instead.
          throw new IllegalArgumentException(
              "Unknown denominator type: " + samplePercent.getDenominator());
      }
      return (int) Math.min(rate, 1_000_000L);
    }

    boolean shouldExecute() {
      if (ratePerMillion >= 1_000_000) {
        return true;
      }
      if (ratePerMillion <= 0) {
        return false;
      }
      return random.nextInt(1_000_000) < ratePerMillion;
    }
  }

  /** A nested filter config and the provider that parsed it. */
  static final class NestedFilter {
    final NamedFilterConfig config;
    final Filter.Provider provider;

    NestedFilter(NamedFilterConfig config, Filter.Provider provider) {
      this.config = config;
      this.provider = provider;
    }
  }

  /**
   * A {@link FilterDelegate} whose nested filter interceptors (or side-support failure status) have
   * already been built at configuration time.
   *
   * <p>Per RPC we only need to evaluate {@link FilterDelegate#shouldExecute} and reuse the
   * pre-built {@code interceptors} result; a side-support error is deliberately deferred to RPC
   * time so that a delegate which is never sampled in never fails anything.
   */
  private static final class ResolvedDelegate<I> {
    final FilterDelegate delegate;
    final StatusOr<List<I>> interceptors;

    ResolvedDelegate(FilterDelegate delegate, StatusOr<List<I>> interceptors) {
      this.delegate = delegate;
      this.interceptors = interceptors;
    }
  }

  @Override
  public ClientInterceptor buildClientInterceptor(FilterConfig config,
      @Nullable FilterConfig overrideConfig, ScheduledExecutorService scheduler) {
    Preconditions.checkNotNull(config, "config");
    CompositeFilterConfig effective =
        (CompositeFilterConfig) Filter.effectiveConfig(config, overrideConfig);
    if (effective.matcher == null) {
      return null;
    }

    final Map<TypedExtensionConfig, ResolvedDelegate<ClientInterceptor>> resolvedDelegates =
        buildResolvedDelegates(
            effective,
            /* isServer= */ false,
            (filter, childCfg) ->
                filter.buildClientInterceptor(childCfg, /* overrideConfig= */ null, scheduler));

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
    CompositeFilterConfig effective =
        (CompositeFilterConfig) Filter.effectiveConfig(config, overrideConfig);
    if (effective.matcher == null) {
      return null;
    }

    final Map<TypedExtensionConfig, ResolvedDelegate<ServerInterceptor>> resolvedDelegates =
        buildResolvedDelegates(
            effective,
            /* isServer= */ true,
            (filter, childCfg) ->
                filter.buildServerInterceptor(childCfg, /* overrideConfig= */ null));

    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        StatusOr<List<ServerInterceptor>> selected = selectInterceptors(
            effective.matcher,
            resolvedDelegates,
            headers,
            call.getMethodDescriptor().getFullMethodName(),
            call.getAuthority());
        if (!selected.hasValue()) {
          call.close(selected.getStatus(), new Metadata());
          return new ServerCall.Listener<ReqT>() {};
        }
        List<ServerInterceptor> interceptors = selected.getValue();
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
   * Whether any client-supported nested filter chain in the effective config reads request
   * payloads, so {@code XdsNameResolver} can install {@code RawMessageClientInterceptor} per route.
   */
  @Override
  public boolean requiresRequestPayloadAccess(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    return anyNestedFilter(config, overrideConfig, (filter, nestedConfig) ->
        filter.requiresRequestPayloadAccess(nestedConfig, /* overrideConfig= */ null));
  }

  /** As {@link #requiresRequestPayloadAccess}, for response payloads. */
  @Override
  public boolean requiresResponsePayloadAccess(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    return anyNestedFilter(config, overrideConfig, (filter, nestedConfig) ->
        filter.requiresResponsePayloadAccess(nestedConfig, /* overrideConfig= */ null));
  }

  private boolean anyNestedFilter(FilterConfig config, @Nullable FilterConfig overrideConfig,
      BiPredicate<Filter, FilterConfig> predicate) {
    CompositeFilterConfig effective =
        (CompositeFilterConfig) Filter.effectiveConfig(config, overrideConfig);
    if (effective.matcher == null) {
      return false;
    }
    for (FilterDelegate delegate : effective.delegates.values()) {
      boolean supportedOnClient = true;
      for (NestedFilter nested : delegate.delegates) {
        if (!nested.provider.isClientFilter()) {
          supportedOnClient = false;
          break;
        }
      }
      if (!supportedOnClient) {
        continue;
      }
      for (NestedFilter nested : delegate.delegates) {
        if (predicate.test(nestedFilter(nested.config), nested.config.filterConfig)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Builds nested interceptors once per configuration update, checking side support across the
   * entire chain before building any child interceptor.
   */
  private <I> Map<TypedExtensionConfig, ResolvedDelegate<I>> buildResolvedDelegates(
      CompositeFilterConfig effective,
      boolean isServer,
      BiFunction<Filter, FilterConfig, I> buildInterceptor) {
    Map<TypedExtensionConfig, ResolvedDelegate<I>> resolved = new LinkedHashMap<>();
    for (Map.Entry<TypedExtensionConfig, FilterDelegate> entry : effective.delegates.entrySet()) {
      FilterDelegate delegate = entry.getValue();
      Status unsupportedError = null;
      for (NestedFilter nested : delegate.delegates) {
        boolean supported =
            isServer ? nested.provider.isServerFilter() : nested.provider.isClientFilter();
        if (!supported) {
          String label = nested.config.name.isEmpty()
              ? nested.config.filterConfig.typeUrl() : nested.config.name;
          unsupportedError = Status.UNAVAILABLE.withDescription(
              "Filter " + label + " is not supported on "
                  + (isServer ? "server" : "client") + " side");
          break;
        }
      }
      if (unsupportedError != null) {
        resolved.put(
            entry.getKey(),
            new ResolvedDelegate<>(delegate, StatusOr.fromStatus(unsupportedError)));
        continue;
      }
      List<I> interceptors = new ArrayList<>(delegate.delegates.size());
      for (NestedFilter nested : delegate.delegates) {
        I interceptor =
            buildInterceptor.apply(nestedFilter(nested.config), nested.config.filterConfig);
        if (interceptor != null) {
          interceptors.add(interceptor);
        }
      }
      resolved.put(
          entry.getKey(),
          new ResolvedDelegate<>(
              delegate, StatusOr.fromValue(Collections.unmodifiableList(interceptors))));
    }
    return Collections.unmodifiableMap(resolved);
  }

  /**
   * The nested filter instance for {@code nested}, obtained from the registry that owns every
   * filter instance of this HCM. The registry creates it from {@link
   * CompositeFilterConfig#nestedFilterConfigs()}, reuses it across updates so that state such as
   * a cache or a side channel survives them, and closes it once no effective config uses it.
   */
  private Filter nestedFilter(NamedFilterConfig nested) {
    Function<NamedFilterConfig, Filter> lookup = Preconditions.checkNotNull(filterLookup,
        "CompositeFilter needs FilterContext.filterLookup() to build interceptors; instances "
            + "must be created by the xDS resolver/server (3-arg FilterContext.create)");
    return Preconditions.checkNotNull(lookup.apply(nested),
        "No filter instance for nested filter %s", nested.filterStateKey());
  }

  /**
   * Evaluates {@code matcher} and {@link FilterDelegate#shouldExecute} for one RPC, returning the
   * pre-built nested interceptors to run (empty when skipped or not sampled) or the {@link Status}
   * to fail the RPC with. The nested filters and their interceptors were already built once at
   * configuration time.
   */
  private static <I> StatusOr<List<I>> selectInterceptors(
      UnifiedMatcher matcher,
      Map<TypedExtensionConfig, ResolvedDelegate<I>> delegatesMap,
      Metadata headers,
      String fullMethodName,
      @Nullable String host) {
    // ":method" is the HTTP method, which is always POST for gRPC. The gRPC full method name is
    // carried by ":path" alone.
    MatchContext context = MatchContext.newBuilder()
        .setMetadata(headers)
        .setMethod("POST")
        .setPath("/" + fullMethodName)
        .setHost(host)
        .build();

    MatchResult matchResult;
    try {
      matchResult = matcher.match(context);
    } catch (RuntimeException e) {
      return StatusOr.fromStatus(Status.UNAVAILABLE
          .withDescription("composite filter matcher evaluation failed")
          .withCause(e));
    }
    if (matchResult == null || !matchResult.matched) {
      logger.log(Level.FINE, "No match in composite filter matcher tree for {0}", fullMethodName);
      return StatusOr.fromStatus(
          Status.UNAVAILABLE.withDescription("no match found in composite filter"));
    }
    if (matchResult.actions.size() != 1) {
      return StatusOr.fromStatus(Status.INTERNAL.withDescription(
          "composite filter matcher returned unexpected action count: "
              + matchResult.actions.size()));
    }
    ResolvedDelegate<I> resolved = delegatesMap.get(matchResult.actions.get(0));
    if (resolved == null) {
      return StatusOr.fromStatus(Status.INTERNAL.withDescription(
          "no filter chain found for matched composite filter action"));
    }
    if (!resolved.delegate.shouldExecute()) {
      logger.log(Level.FINE, "Matched action not sampled, skipping nested filters for {0}",
          fullMethodName);
      return StatusOr.fromValue(Collections.emptyList());
    }
    if (resolved.interceptors.hasValue() && logger.isLoggable(Level.FINE)) {
      logger.log(Level.FINE, "Composite filter running {0} nested interceptor(s) for {1}",
          new Object[] {resolved.interceptors.getValue().size(), fullMethodName});
    }
    return resolved.interceptors;
  }

  /**
   * Relies on the {@link ClientCall} contract: {@code start()} must complete before any method
   * other than {@code cancel()} is called, so a {@code cancel()} racing {@code start()} from
   * another thread is outside the contract. The delegate is published before it is started, so
   * whatever the listener sends from inside {@code start()} already reaches it.
   */
  private static final class CompositeClientCall<ReqT, RespT>
      extends ForwardingClientCall<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> method;
    private final CallOptions callOptions;
    private final Channel next;
    private final UnifiedMatcher matcher;
    private final Map<TypedExtensionConfig, ResolvedDelegate<ClientInterceptor>> delegatesMap;
    private final Context context = Context.current();
    // Null until start().
    private volatile ClientCall<ReqT, RespT> delegate;
    // cancel() is the only method allowed before start(); it is recorded and reported from start().
    private volatile Status cancelledBeforeStart;

    CompositeClientCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions,
        Channel next, UnifiedMatcher matcher,
        Map<TypedExtensionConfig, ResolvedDelegate<ClientInterceptor>> delegatesMap) {
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

          @Override
          public boolean isReady() {
            return false;
          }
        };

    @SuppressWarnings("unchecked")
    private static <ReqT, RespT> ClientCall<ReqT, RespT> noopCall() {
      return (ClientCall<ReqT, RespT>) NOOP_CALL;
    }

    @Override
    protected ClientCall<ReqT, RespT> delegate() {
      ClientCall<ReqT, RespT> call = delegate;
      Preconditions.checkState(call != null, "Not started");
      return call;
    }

    @Override
    public boolean isReady() {
      ClientCall<ReqT, RespT> call = delegate;
      return call != null && call.isReady();
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      ClientCall<ReqT, RespT> call = delegate;
      if (call == null) {
        cancelledBeforeStart = Status.CANCELLED.withDescription(message).withCause(cause);
        return;
      }
      call.cancel(message, cause);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      Preconditions.checkState(delegate == null, "Already started");
      if (cancelledBeforeStart != null) {
        // Never create the real call: it would only be cancelled, and an unstarted call holds no
        // transport resources.
        failCall(responseListener, cancelledBeforeStart);
        return;
      }

      String host = callOptions.getAuthority() != null
          ? callOptions.getAuthority() : next.authority();
      StatusOr<List<ClientInterceptor>> selected =
          selectInterceptors(matcher, delegatesMap, headers, method.getFullMethodName(), host);
      if (!selected.hasValue()) {
        failCall(responseListener, selected.getStatus());
        return;
      }
      List<ClientInterceptor> interceptors = selected.getValue();
      // Restore the Context captured at interceptCall() time while creating and starting the
      // deferred downstream call, matching Channel.newCall() / DelayedClientCall semantics.
      Context previous = context.attach();
      try {
        // interceptForward, not intercept: A103 specifies filter_chain as "a chain of filters to
        // call, in order", so the first entry has to run first. ClientInterceptors.intercept makes
        // the *last* interceptor outermost, which would run the chain backwards.
        ClientCall<ReqT, RespT> realCall = interceptors.isEmpty()
            ? next.newCall(method, callOptions)
            : ClientInterceptors.interceptForward(next, interceptors).newCall(method, callOptions);
        // With a direct executor the transport may deliver onReady inside start(), and whatever the
        // listener then sends must reach realCall.
        delegate = realCall;
        realCall.start(responseListener, headers);
      } finally {
        context.detach(previous);
      }
    }

    private void failCall(Listener<RespT> responseListener, Status status) {
      delegate = noopCall();
      Executor executor = callOptions.getExecutor();
      if (executor == null) {
        executor = MoreExecutors.directExecutor();
      }
      executor.execute(
          () -> {
            Context previous = context.attach();
            try {
              responseListener.onClose(status, new Metadata());
            } finally {
              context.detach(previous);
            }
          });
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("delegate", delegate).toString();
    }
  }
}
