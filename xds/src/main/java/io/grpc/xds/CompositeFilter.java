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
import io.envoyproxy.envoy.extensions.filters.common.matcher.action.v3.SkipFilter;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.InternalServerInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * The composite filter of gRFC A103: a matcher tree that selects which nested filters to run for
 * each RPC.
 *
 * <p>The composite holds no instance state of its own. Nested filter instances are owned by the
 * resolver's or server's filter registry - the same flat map that owns the top-level filters - and
 * are shared HCM-wide by {@code name_type}: a nested filter and a top-level filter with the same
 * name and type, or the same name nested under two composites, are one instance. The composite
 * obtains them through {@link FilterContext#filterAcquirer()} while building interceptors.
 */
final class CompositeFilter implements Filter {

  static final String TYPE_URL_EXTENSION_WITH_MATCHER =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcher";
  static final String TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE =
      "type.googleapis.com/envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute";
  private static final Logger logger = Logger.getLogger(CompositeFilter.class.getName());

  @Nullable
  private final Function<NamedFilterConfig, Filter> filterAcquirer;

  CompositeFilter(@Nullable Function<NamedFilterConfig, Filter> filterAcquirer) {
    this.filterAcquirer = filterAcquirer;
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
      return new CompositeFilter(context.filterAcquirer());
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfig(
        Message rawProtoMessage, FilterConfigParseContext context) {
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError(
            "Invalid message type: " + rawProtoMessage.getClass().getName());
      }
      if (context.recursionDepth() >= FilterConfigParseContext.MAX_RECURSION_DEPTH) {
        return ConfigOrError.fromError("Maximum recursion depth of "
            + FilterConfigParseContext.MAX_RECURSION_DEPTH + " exceeded");
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
        // per-route override may still supply one. The deprecated `matcher` field is ignored,
        // per A103.
        return parseMatcherConfig(proto.hasXdsMatcher() ? proto.getXdsMatcher() : null, context);
      } catch (InvalidProtocolBufferException e) {
        return ConfigOrError.fromError("Invalid proto: " + e);
      }
    }

    @Override
    public ConfigOrError<CompositeFilterConfig> parseFilterConfigOverride(
        Message rawProtoMessage, FilterConfigParseContext context) {
      if (!(rawProtoMessage instanceof Any)) {
        return ConfigOrError.fromError(
            "Invalid message type: " + rawProtoMessage.getClass().getName());
      }
      if (context.recursionDepth() >= FilterConfigParseContext.MAX_RECURSION_DEPTH) {
        return ConfigOrError.fromError("Maximum recursion depth of "
            + FilterConfigParseContext.MAX_RECURSION_DEPTH + " exceeded");
      }
      try {
        Any any = (Any) rawProtoMessage;
        if (!any.is(ExtensionWithMatcherPerRoute.class)) {
          return ConfigOrError.fromError(
              "Expected ExtensionWithMatcherPerRoute but got: " + any.getTypeUrl());
        }
        ExtensionWithMatcherPerRoute proto = any.unpack(ExtensionWithMatcherPerRoute.class);
        // A103 requires xds_matcher to be "validated the same way as the corresponding field in
        // the top-level config", and there an absent matcher is permitted and makes the filter a
        // no-op. Since the override replaces the top-level matcher outright, an absent one
        // replaces it with nothing: the filter stops matching on this route. Rejecting it here
        // would NACK a config the spec calls valid.
        return parseMatcherConfig(proto.hasXdsMatcher() ? proto.getXdsMatcher() : null, context);
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
      if (hasKeepMatching(matcherProto)) {
        return ConfigOrError.fromError(
            "keep_matching is not permitted anywhere in the composite filter matcher tree");
      }
      try {
        Map<TypedExtensionConfig, FilterDelegate> delegates = new HashMap<>();
        collectDelegates(matcherProto, delegates, context);
        UnifiedMatcher matcher = UnifiedMatcher.fromProto(
            matcherProto, this::validateActionTypeUrl);
        return ConfigOrError.fromConfig(
            new CompositeFilterConfig(matcher, delegates, matcherProto));
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

    private void collectDelegates(Matcher matcher, Map<TypedExtensionConfig, FilterDelegate> map,
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

    private void collectOnMatch(Matcher.OnMatch onMatch,
        Map<TypedExtensionConfig, FilterDelegate> map,
        FilterConfigParseContext context) {
      if (onMatch.hasMatcher()) {
        collectDelegates(onMatch.getMatcher(), map, context);
      } else if (onMatch.hasAction()) {
        TypedExtensionConfig action = onMatch.getAction();
        // Keyed by the action message rather than by action.name: `name` is an opaque identifier
        // that "is not used to select the extension", and nothing constrains it to be unique
        // across actions, so two distinct actions may legitimately share one. Identical actions
        // collapse to a single entry, which is intended - the same action reached from several
        // matcher branches is one delegate, parsed once.
        if (!map.containsKey(action)) {
          FilterDelegate delegate = createFilterDelegate(action, context);
          if (delegate != null) {
            map.put(action, delegate);
          }
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
          // SkipFilter declares no fields, so there is nothing to read - but the payload is still
          // unpacked, so that malformed wire bytes are rejected rather than silently treated as
          // a skip.
          try {
            SkipFilter unused = actionAny.unpack(SkipFilter.class);
          } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Could not parse SkipFilter action", e);
          }
          return new FilterDelegate(Collections.emptyList(), null);
        }
        if (!actionAny.is(ExecuteFilterAction.class)) {
          throw new IllegalArgumentException(
              "Expected ExecuteFilterAction or SkipFilter but got: " + actionAny.getTypeUrl());
        }
        ExecuteFilterAction executeAction = actionAny.unpack(ExecuteFilterAction.class);
        // A103 requires default_value to be present whenever sample_percent is; without this
        // check the unset message would read as 0%, silently disabling the action instead of
        // rejecting the config. runtime_key is ignored, per A103, because gRPC has no runtime
        // system.
        FractionalPercent samplePercent = null;
        if (executeAction.hasSamplePercent()) {
          if (!executeAction.getSamplePercent().hasDefaultValue()) {
            throw new IllegalArgumentException(
                "ExecuteFilterAction.sample_percent.default_value: field not set");
          }
          samplePercent = executeAction.getSamplePercent().getDefaultValue();
        }
        List<io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig> childConfigs =
            new ArrayList<>();
        // A103 makes it an error only if *neither* field is set. A filter_chain that is set but
        // empty is legal and produces an action that runs no filters, which is the same
        // observable behaviour as SkipFilter.
        if (executeAction.hasFilterChain()) {
          childConfigs.addAll(executeAction.getFilterChain().getTypedConfigList());
        } else if (executeAction.hasTypedConfig()) {
          childConfigs.add(executeAction.getTypedConfig());
        } else {
          throw new IllegalArgumentException(
              "ExecuteFilterAction must specify either typed_config or filter_chain");
        }
        List<DelegateEntry> delegates = new ArrayList<>();
        for (io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig childFilterConfig
            : childConfigs) {
          String name = childFilterConfig.getName();
          // TypedExtensionConfig.name carries `(validate.rules).string = {min_len: 1}`, and the
          // name is what nested filter instances are keyed by, so an absent one is not merely
          // invalid but unusable - every anonymous filter of a given type would alias onto one
          // instance. (C-core does not check this; it is a deliberate, stricter check.)
          //
          // Nothing else about the name is validated. Nested instances live in the same HCM-wide
          // map as top-level ones, keyed by name and type, so a repeated name *means* a shared
          // instance - the A83 mechanism for sharing state such as a gcp_authn cache across
          // several match arms. A collision check here could only ever see one composite's
          // config, never a same-named top-level filter, a sibling composite or an RDS override
          // parsed as a separate resource, and C-core accepts all of these.
          if (name.isEmpty()) {
            throw new IllegalArgumentException("Nested filter is missing a name: "
                + childFilterConfig.getTypedConfig().getTypeUrl());
          }
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
          // A103 bans terminal filters under a composite filter. The router filter is the only
          // terminal filter, matching XdsListenerResource#isTerminalFilter; both will need
          // updating together if that ever stops being true.
          if (RouterFilter.TYPE_URL.equals(typeUrl)) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter: " + typeUrl);
          }
          Filter.Provider provider = registryLookup.apply(typeUrl);
          if (provider == null) {
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
          // Defence in depth: the type URL check above can be evaded by a provider registered
          // under another type URL that nonetheless returns the router config.
          if (parsed.config == RouterFilter.ROUTER_CONFIG) {
            throw new IllegalArgumentException(
                "Nested filter cannot be a terminal filter: " + typeUrl);
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
      return "CompositeFilterConfig{matcher=" + (matcherProto == null ? "none" : "set")
          + ", delegates=" + delegates.size() + "}";
    }
  }

  static final class FilterDelegate {
    final List<DelegateEntry> delegates;
    private final int ratePerMillion;
    private final ThreadSafeRandom random;

    FilterDelegate(List<DelegateEntry> delegates, @Nullable FractionalPercent samplePercent) {
      this(delegates, samplePercent, ThreadSafeRandom.ThreadSafeRandomImpl.instance);
    }

    FilterDelegate(List<DelegateEntry> delegates, @Nullable FractionalPercent samplePercent,
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

  static final class DelegateEntry {
    final Filter.Provider provider;
    final FilterConfig config;
    final String name;
    final NamedFilterConfig namedConfig;

    DelegateEntry(Filter.Provider provider, FilterConfig config, String name) {
      this.provider = provider;
      this.config = config;
      this.name = name;
      this.namedConfig = new NamedFilterConfig(name, config);
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
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    final Map<TypedExtensionConfig, ResolvedDelegate<ClientInterceptor>> resolvedDelegates =
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
    CompositeFilterConfig effective = getEffectiveConfig(config, overrideConfig);
    if (effective == null || effective.matcher == null) {
      return null;
    }

    final Map<TypedExtensionConfig, ResolvedDelegate<ServerInterceptor>> resolvedDelegates =
        resolveAll(effective, this::resolveServerDelegate);

    return new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        // ":method" is the HTTP method, which is always POST for gRPC. The gRPC full method name
        // is carried by ":path" alone.
        MatchContext context = MatchContext.newBuilder()
            .setMetadata(headers)
            .setMethod("POST")
            .setPath("/" + call.getMethodDescriptor().getFullMethodName())
            .setHost(call.getAuthority())
            .build();

        MatchResult matchResult = effective.matcher.match(context);
        if (matchResult == null || !matchResult.matched) {
          logger.log(Level.FINE, "No match in composite filter matcher tree for {0}",
              call.getMethodDescriptor().getFullMethodName());
          call.close(
              Status.UNAVAILABLE.withDescription("no match found in composite filter"),
              new Metadata());
          return new ServerCall.Listener<ReqT>() {};
        }

        // Only matcher evaluation and sampling happen per RPC. The nested filters and their
        // interceptors were already built once above, at configuration time.
        List<ServerInterceptor> interceptors = new ArrayList<>();
        for (ResolvedDelegate<ServerInterceptor> resolved
            : resolveDelegates(matchResult, resolvedDelegates)) {
          if (!resolved.delegate.shouldExecute()) {
            logger.log(Level.FINE, "Matched action not sampled, skipping nested filters for {0}",
                call.getMethodDescriptor().getFullMethodName());
            continue;
          }
          if (resolved.error != null) {
            call.close(resolved.error, new Metadata());
            return new ServerCall.Listener<ReqT>() {};
          }
          interceptors.addAll(resolved.interceptors);
        }

        if (logger.isLoggable(Level.FINE)) {
          logger.log(Level.FINE, "Composite filter running {0} nested interceptor(s) for {1}",
              new Object[] {interceptors.size(),
                  call.getMethodDescriptor().getFullMethodName()});
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
   * <p>Creating nested filters per RPC would defeat the caches and shared connections that
   * {@link Filter} implementations are documented to own, and would violate
   * {@link Filter.Provider#newInstance}'s lifecycle contract.
   */
  private <I> Map<TypedExtensionConfig, ResolvedDelegate<I>> resolveAll(
      CompositeFilterConfig effective, Function<FilterDelegate, ResolvedDelegate<I>> resolver) {
    Map<TypedExtensionConfig, ResolvedDelegate<I>> resolved = new HashMap<>();
    for (Map.Entry<TypedExtensionConfig, FilterDelegate> entry : effective.delegates.entrySet()) {
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
      ServerInterceptor interceptor = nestedFilter(entry)
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
      ClientInterceptor interceptor = nestedFilter(entry)
          .buildClientInterceptor(entry.config, /* overrideConfig= */ null, scheduler);
      if (interceptor != null) {
        interceptors.add(interceptor);
      }
    }
    return ResolvedDelegate.of(delegate, interceptors);
  }

  /**
   * The nested filter instance for {@code entry}, obtained from the registry that owns every
   * filter instance of this HCM. The registry creates it on first use, reuses it across updates -
   * a nested filter may hold a cache or a side channel that exists precisely to survive them - and
   * closes it once an update no longer reaches it.
   */
  private Filter nestedFilter(DelegateEntry entry) {
    return Preconditions.checkNotNull(filterAcquirer,
        "CompositeFilter needs FilterContext.filterAcquirer() to build interceptors; instances "
            + "must be created by the xDS resolver/server (3-arg FilterContext.create)")
        .apply(entry.namedConfig);
  }

  private static CompositeFilterConfig getEffectiveConfig(
      FilterConfig config, @Nullable FilterConfig overrideConfig) {
    // A per-route override replaces the top-level config outright, per A103 - including its
    // matcher, which may legitimately be absent on either path. Callers guard for that: both
    // buildClientInterceptor and buildServerInterceptor return no interceptor when the effective
    // matcher is null, which is the no-op passthrough A103 asks for.
    return (CompositeFilterConfig) (overrideConfig != null ? overrideConfig : config);
  }

  static <I> List<ResolvedDelegate<I>> resolveDelegates(@Nullable MatchResult matchResult,
      Map<TypedExtensionConfig, ResolvedDelegate<I>> delegatesMap) {
    if (matchResult == null || !matchResult.matched || matchResult.actions.isEmpty()) {
      return Collections.emptyList();
    }
    List<ResolvedDelegate<I>> list = new ArrayList<>();
    for (TypedExtensionConfig action : matchResult.actions) {
      ResolvedDelegate<I> d = delegatesMap.get(action);
      if (d != null) {
        list.add(d);
      }
    }
    return Collections.unmodifiableList(list);
  }

  private static final class CompositeClientCall<ReqT, RespT>
      extends ForwardingClientCall<ReqT, RespT> {
    private final MethodDescriptor<ReqT, RespT> method;
    private final CallOptions callOptions;
    private final Channel next;
    private final UnifiedMatcher matcher;
    private final Map<TypedExtensionConfig, ResolvedDelegate<ClientInterceptor>> delegatesMap;
    private final Object lock = new Object();
    private ClientCall<ReqT, RespT> delegate;
    private boolean started;
    private Status cancelStatus;
    private int pendingRequests;

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
    public void request(int numMessages) {
      ClientCall<ReqT, RespT> callToRequest;
      synchronized (lock) {
        Preconditions.checkState(started, "Not started");
        if (delegate == null) {
          // start() is still resolving the matcher tree and building the real call. request() is
          // the one ClientCall method that may be called from another thread, so the demand has
          // to be remembered and replayed once the real call has been started; dropping it on the
          // no-op call would stall the RPC forever.
          pendingRequests += numMessages;
          return;
        }
        callToRequest = delegate;
      }
      callToRequest.request(numMessages);
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
      Status cancelledBeforeStart;
      synchronized (lock) {
        Preconditions.checkState(!started, "Already started");
        started = true;
        cancelledBeforeStart = cancelStatus;
        if (cancelledBeforeStart != null) {
          delegate = noopCall();
        }
      }
      if (cancelledBeforeStart != null) {
        responseListener.onClose(cancelledBeforeStart, new Metadata());
        return;
      }

      String host = callOptions.getAuthority() != null
          ? callOptions.getAuthority() : next.authority();
      MatchContext context = MatchContext.newBuilder()
          .setMetadata(headers)
          .setMethod("POST")
          .setPath("/" + method.getFullMethodName())
          .setHost(host)
          .build();

      MatchResult matchResult = matcher.match(context);
      if (matchResult == null || !matchResult.matched) {
        logger.log(Level.FINE, "No match in composite filter matcher tree for {0}",
            method.getFullMethodName());
        failCall(responseListener,
            Status.UNAVAILABLE.withDescription("no match found in composite filter"));
        return;
      }

      // Only matcher evaluation and sampling happen per RPC. The nested filters and their
      // interceptors were already built once at configuration time; see
      // CompositeFilter#buildClientInterceptor.
      List<ClientInterceptor> interceptors = new ArrayList<>();
      for (ResolvedDelegate<ClientInterceptor> resolved
          : resolveDelegates(matchResult, delegatesMap)) {
        if (!resolved.delegate.shouldExecute()) {
          logger.log(Level.FINE, "Matched action not sampled, skipping nested filters for {0}",
              method.getFullMethodName());
          continue;
        }
        if (resolved.error != null) {
          failCall(responseListener, resolved.error);
          return;
        }
        interceptors.addAll(resolved.interceptors);
      }

      if (logger.isLoggable(Level.FINE)) {
        logger.log(Level.FINE, "Composite filter running {0} nested interceptor(s) for {1}",
            new Object[] {interceptors.size(), method.getFullMethodName()});
      }
      // interceptForward, not intercept: A103 specifies filter_chain as "a chain of filters to
      // call, in order", so the first entry has to run first. ClientInterceptors.intercept makes
      // the *last* interceptor outermost, which would run the chain backwards.
      ClientCall<ReqT, RespT> realCall = interceptors.isEmpty()
          ? next.newCall(method, callOptions)
          : ClientInterceptors.interceptForward(next, interceptors).newCall(method, callOptions);

      Status cancelledMidStart;
      synchronized (lock) {
        cancelledMidStart = cancelStatus;
        if (cancelledMidStart != null) {
          delegate = noopCall();
        }
      }
      if (cancelledMidStart != null) {
        // Deliberately do not start realCall. ClientCallImpl.startInternal rejects a call that was
        // already cancelled, and starting it only to cancel it would open a stream and send
        // request headers for an RPC nobody is waiting for. An unstarted call holds no transport
        // resources, so dropping it leaks nothing; we only have to close the listener ourselves.
        responseListener.onClose(cancelledMidStart, new Metadata());
        return;
      }

      // Start before publishing. While delegate is still null a concurrent cancel() only records
      // cancelStatus, so it cannot reach a call that has not been started yet, and a concurrent
      // request() buffers its demand instead of dropping it.
      realCall.start(responseListener, headers);

      int bufferedRequests;
      Status cancelledDuringStart;
      synchronized (lock) {
        delegate = realCall;
        bufferedRequests = pendingRequests;
        pendingRequests = 0;
        cancelledDuringStart = cancelStatus;
      }
      if (bufferedRequests > 0) {
        realCall.request(bufferedRequests);
      }
      if (cancelledDuringStart != null) {
        // A cancel raced with start() and found delegate still null, so it never reached realCall.
        // Any cancel arriving after this block sees the published delegate and cancels it itself.
        realCall.cancel(cancelledDuringStart.getDescription(), cancelledDuringStart.getCause());
      }
    }

    private void failCall(Listener<RespT> responseListener, Status status) {
      synchronized (lock) {
        delegate = noopCall();
      }
      responseListener.onClose(status, new Metadata());
    }
  }
}
