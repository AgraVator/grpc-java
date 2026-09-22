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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.udpa.udpa.type.v1.TypedStruct;
import com.github.xds.type.matcher.v3.Matcher;
import com.github.xds.type.matcher.v3.StringMatcher;
import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.core.v3.TrafficDirection;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.config.listener.v3.ApiListener;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.FilterChainMatch;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.NonForwardingAction;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.common.matcher.action.v3.SkipFilter;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.FaultAbort;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.HTTPFault;
import io.envoyproxy.envoy.extensions.filters.http.rbac.v3.RBAC;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;
import io.envoyproxy.envoy.type.matcher.v3.HttpRequestHeaderMatchInput;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MetricRecorder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import io.grpc.xds.Filter.FilterConfig;
import io.grpc.xds.Filter.FilterConfigParseContext;
import io.grpc.xds.Filter.FilterContext;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData;
import io.grpc.xds.internal.matcher.UnifiedMatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Enclosed.class)
public class CompositeFilterTest {

  @RunWith(JUnit4.class)
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class UnitTest {

    private static final String FAKE_TYPE_URL = "type.googleapis.com/fake";
    private static final String FAKE_UNSUPPORTED_TYPE_URL = "type.googleapis.com/fake.unsupported";
    private static final String FAKE_FAILING_PARSE_TYPE_URL = "type.googleapis.com/fake.failing";
    private static final String EXECUTE_ACTION_TYPE_URL =
        "type.googleapis.com/envoy.extensions.filters.http.composite.v3.ExecuteFilterAction";

    private CompositeFilter.Provider provider;

    @Mock
    private Filter.Provider fakeProvider;
    @Mock
    private Filter.Provider fakeUnsupportedProvider;
    @Mock
    private Filter.Provider fakeFailingProvider;
    @Mock
    private Filter fakeFilter;
    @Mock
    private ClientInterceptor fakeClientInterceptor;
    @Mock
    private ServerInterceptor fakeServerInterceptor;
    @Mock
    private FilterConfig fakeConfig;

    @Before
    @SuppressWarnings("deprecation")
    public void setUp() {
      MockitoAnnotations.initMocks(this);
      System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");

      when(fakeProvider.typeUrls()).thenReturn(new String[]{FAKE_TYPE_URL});
      when(fakeProvider.isClientFilter()).thenReturn(true);
      when(fakeProvider.isServerFilter()).thenReturn(true);
      when(fakeConfig.typeUrl()).thenReturn(FAKE_TYPE_URL);
      ConfigOrError<? extends FilterConfig> configRes = ConfigOrError.fromConfig(fakeConfig);
      when(fakeProvider.parseFilterConfig(any(com.google.protobuf.Message.class), any()))
          .thenReturn((ConfigOrError) configRes);
      when(fakeProvider.newInstance(any(FilterContext.class))).thenReturn(fakeFilter);
      when(fakeFilter.buildClientInterceptor(any(), any(), any()))
          .thenReturn(fakeClientInterceptor);
      when(fakeFilter.buildServerInterceptor(any(), any())).thenReturn(fakeServerInterceptor);

      when(fakeUnsupportedProvider.typeUrls()).thenReturn(new String[]{FAKE_UNSUPPORTED_TYPE_URL});
      when(fakeUnsupportedProvider.isClientFilter()).thenReturn(false);
      when(fakeUnsupportedProvider.isServerFilter()).thenReturn(false);
      when(fakeUnsupportedProvider.parseFilterConfig(
          any(com.google.protobuf.Message.class), any()))
          .thenReturn((ConfigOrError) configRes);

      when(fakeFailingProvider.typeUrls()).thenReturn(new String[]{FAKE_FAILING_PARSE_TYPE_URL});
      when(fakeFailingProvider.isClientFilter()).thenReturn(true);
      when(fakeFailingProvider.isServerFilter()).thenReturn(true);
      when(fakeFailingProvider.parseFilterConfig(any(com.google.protobuf.Message.class), any()))
          .thenReturn(ConfigOrError.fromError("Child filter config parsing failed intentionally"));

      provider = new CompositeFilter.Provider(typeUrl -> {
        if (FAKE_TYPE_URL.equals(typeUrl)) {
          return fakeProvider;
        }
        if (FAKE_UNSUPPORTED_TYPE_URL.equals(typeUrl)) {
          return fakeUnsupportedProvider;
        }
        if (FAKE_FAILING_PARSE_TYPE_URL.equals(typeUrl)) {
          return fakeFailingProvider;
        }
        return FilterRegistry.getDefaultRegistry().get(typeUrl);
      });
    }

    @After
    public void tearDown() {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }

    private FilterConfigParseContext getFilterContext() {
      return FilterConfigParseContext.builder()
          .bootstrapInfo(Bootstrapper.BootstrapInfo.builder()
              .servers(Collections.singletonList(
                  Bootstrapper.ServerInfo.create("test_target", Collections.emptyMap())))
              .node(EnvoyProtoData.Node.newBuilder().build())
              .build())
          .serverInfo(Bootstrapper.ServerInfo.create(
              "test_target", Collections.emptyMap(), false, true, false, false))
          .build();
    }

    /**
     * Stands in for the {@code activeFilters} map that {@code XdsNameResolver} and
     * {@code XdsServerWrapper} own: the framework creates every nested filter up front, keyed by
     * {@code NamedFilterConfig.filterStateKey()}, and {@link CompositeFilter} only ever reads it.
     */
    private final Map<String, Filter> activeFilters = new HashMap<>();

    private CompositeFilter newFilter(String name) {
      return (CompositeFilter) provider.newInstance(
          FilterContext.create(
              name,
              mock(MetricRecorder.class),
              key -> activeFilters.computeIfAbsent(key, k -> {
                int sep = k.lastIndexOf('_');
                String nestedName = k.substring(0, sep);
                String typeUrl = k.substring(sep + 1);
                Filter.Provider p = FAKE_UNSUPPORTED_TYPE_URL.equals(typeUrl)
                    ? fakeUnsupportedProvider : fakeProvider;
                return p.newInstance(
                    FilterContext.create(nestedName, mock(MetricRecorder.class)));
              })));
    }

    private static ExtensionWithMatcher createExtensionWithMatcher(Matcher matcher) {
      return ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder()
              .setName("composite")
              .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
              .build())
          .setXdsMatcher(matcher)
          .build();
    }

    private MethodDescriptor<Void, Void> createMockMethod() {
      MethodDescriptor.Marshaller<Void> marshaller = mock(MethodDescriptor.Marshaller.class);
      return MethodDescriptor.<Void, Void>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName("service/method")
          .setRequestMarshaller(marshaller)
          .setResponseMarshaller(marshaller)
          .build();
    }

    private static Matcher.OnMatch createExecuteAction(String childName, String typeUrl) {
      return Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_" + childName)
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName(childName)
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(typeUrl)
                              .setValue(Composite.getDefaultInstance().toByteString())
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();
    }

    /**
     * Builds an ExecuteFilterAction whose action name is chosen independently of the child filter
     * name, so that tests can construct two distinct actions that share a name.
     */
    private static Matcher.OnMatch createExecuteActionNamed(
        String actionName, String childName, String typeUrl) {
      return Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName(actionName)
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName(childName)
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(typeUrl)
                              .setValue(Composite.getDefaultInstance().toByteString())
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();
    }

    private static Matcher.MatcherList.FieldMatcher createHeaderFieldMatcher(
        String headerName, String headerValue, Matcher.OnMatch onMatch) {
      return Matcher.MatcherList.FieldMatcher.newBuilder()
          .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
              .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                  .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                      .setName("request_headers")
                      .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                          .setHeaderName(headerName)
                          .build()))
                      .build())
                  .setValueMatch(StringMatcher.newBuilder().setExact(headerValue).build())
                  .build())
              .build())
          .setOnMatch(onMatch)
          .build();
    }

    @Test
    public void providerMethodsCovered() {
      assertThat(provider.typeUrls()).asList().containsExactly(
          CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER,
          CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE
      );
      assertThat(provider.isClientFilter()).isTrue();
      assertThat(provider.isServerFilter()).isTrue();
    }

    @Test
    public void parseFilterConfig_success() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
      assertThat(result.config.matcher).isNotNull();
      assertThat(result.config.delegates).hasSize(1);
      assertThat(Iterables.getOnlyElement(result.config.delegates.keySet()).getName())
          .isEqualTo("action_child");
    }

    @Test
    public void parseFilterConfig_actionsSharingANameAreBothRetained() {
      // An action's `name` is documentation only -- the proto says it "is not used to select the
      // extension" -- and nothing requires it to be unique or even set. Keying delegates by name
      // silently dropped the second action, so a matcher branch could resolve to the wrong child
      // filter, or to none at all.
      Matcher.OnMatch first = createExecuteActionNamed("dup", "childA", FAKE_TYPE_URL);
      Matcher.OnMatch second = createExecuteActionNamed("dup", "childB", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "a", first))
              .addMatchers(createHeaderFieldMatcher("foo", "b", second))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config.delegates).hasSize(2);
      assertThat(result.config.delegates).containsKey(first.getAction());
      assertThat(result.config.delegates).containsKey(second.getAction());
      for (com.github.xds.core.v3.TypedExtensionConfig key : result.config.delegates.keySet()) {
        assertThat(key.getName()).isEqualTo("dup");
      }
    }

    @Test
    public void parseFilterConfig_equalProtosProduceEqualConfigs() {
      // The xDS client decides whether to wake watchers by comparing parsed resources, so a
      // control plane re-sending an unchanged listener must not look like a change.
      ExtensionWithMatcher proto = createExtensionWithMatcher(Matcher.newBuilder()
          .setOnNoMatch(createExecuteAction("child", FAKE_TYPE_URL))
          .build());

      CompositeFilter.CompositeFilterConfig first =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext()).config;
      CompositeFilter.CompositeFilterConfig second =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext()).config;

      assertThat(first).isNotSameInstanceAs(second);
      assertThat(first).isEqualTo(second);
      assertThat(first.hashCode()).isEqualTo(second.hashCode());
    }

    @Test
    public void parseFilterConfig_differentProtosProduceUnequalConfigs() {
      CompositeFilter.CompositeFilterConfig first = provider.parseFilterConfig(
          Any.pack(createExtensionWithMatcher(Matcher.newBuilder()
              .setOnNoMatch(createExecuteAction("child_a", FAKE_TYPE_URL))
              .build())),
          getFilterContext()).config;
      CompositeFilter.CompositeFilterConfig second = provider.parseFilterConfig(
          Any.pack(createExtensionWithMatcher(Matcher.newBuilder()
              .setOnNoMatch(createExecuteAction("child_b", FAKE_TYPE_URL))
              .build())),
          getFilterContext()).config;

      assertThat(first).isNotEqualTo(second);
    }

    @Test
    public void parseFilterConfig_matcherlessConfigsAreEqual() {
      // Two passthrough configs carry no matcher and no delegates, so they are interchangeable.
      ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
              .setName("composite")
              .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
              .build())
          .build();

      CompositeFilter.CompositeFilterConfig first =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext()).config;
      CompositeFilter.CompositeFilterConfig second =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext()).config;

      assertThat(first.matcher).isNull();
      assertThat(first).isEqualTo(second);
    }

    @Test
    public void parseFilterConfig_identicalActionsCollapseToOneDelegate() {
      // Two byte-identical actions describe the same work, so sharing a delegate is intended and
      // keeps the map from growing with every duplicated matcher branch.
      Matcher.OnMatch action = createExecuteActionNamed("same", "child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "a", action))
              .addMatchers(createHeaderFieldMatcher("foo", "b", action))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config.delegates).hasSize(1);
    }

    @Test
    public void parseFilterConfig_actionsWithNoNameAreBothRetained() {
      // gRPC-Java does not enforce min_len:1 on the action name, so an unset name is legal and
      // would previously have collapsed every anonymous action onto the empty-string key.
      Matcher.OnMatch first = createExecuteActionNamed("", "childA", FAKE_TYPE_URL);
      Matcher.OnMatch second = createExecuteActionNamed("", "childB", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "a", first))
              .addMatchers(createHeaderFieldMatcher("foo", "b", second))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config.delegates).hasSize(2);
    }

    @Test
    public void parseFilterConfig_bareCompositeFails() {
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(Composite.getDefaultInstance()), getFilterContext());

      assertThat(result.errorDetail).contains("Expected ExtensionWithMatcher but got");
    }

    @Test
    public void parseFilterConfig_emptyConfigWithExtensionWithMatcherSucceeds() {
      ExtensionWithMatcher protoNoMatcher = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder()
              .setName("composite")
              .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result1 =
          provider.parseFilterConfig(Any.pack(protoNoMatcher), getFilterContext());

      assertThat(result1.errorDetail).isNull();
      assertThat(result1.config).isNotNull();
      assertThat(result1.config.matcher).isNull();

      ExtensionWithMatcher protoEmptyMatcher =
          createExtensionWithMatcher(Matcher.getDefaultInstance());

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result2 =
          provider.parseFilterConfig(Any.pack(protoEmptyMatcher), getFilterContext());

      assertThat(result2.errorDetail).isNull();
      assertThat(result2.config).isNotNull();
      assertThat(result2.config.matcher).isNotNull();
    }

    @Test
    public void whenDisabled_reportedAsUnsupportedRatherThanFailingToParse() {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
      try {
        // The resource layer honours the http_filter's is_optional flag when a filter is
        // unsupported, but treats a config parse error as a fatal NACK. Reporting "disabled" as
        // unsupported therefore lets an optional composite filter be skipped instead of rejecting
        // the entire listener.
        assertThat(provider.isClientFilter()).isFalse();
        assertThat(provider.isServerFilter()).isFalse();
      } finally {
        System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
      }
    }

    @Test
    public void parseFilterConfig_exceedsRecursionLimit() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                  .setOnMatch(matchAction)
                  .build())
              .build())
          .build();

      final Any configAny = Any.pack(createExtensionWithMatcher(matcher));

      when(fakeProvider.parseFilterConfig(any(), any()))
          .thenAnswer(invocation -> {
            FilterConfigParseContext context = invocation.getArgument(1);
            int depth = context.recursionDepth();
            FilterConfigParseContext childContext = context.toBuilder()
                .recursionDepth(depth + 1)
                .build();
            return provider.parseFilterConfig(configAny, childContext);
          });

      ConfigOrError<? extends FilterConfig> result =
          provider.parseFilterConfig(configAny, getFilterContext());

      assertThat(result.errorDetail).contains("Maximum recursion depth of 8 exceeded");
    }

    @Test
    public void parseFilterConfig_keepMatchingFailsValidation() {
      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.newBuilder().setTypeUrl(FAKE_TYPE_URL).build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void parseFilterConfig_nestedTerminalFilterFailsValidation() {
      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("terminal_action")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("router")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(RouterFilter.TYPE_URL)
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void parseFilterConfig_withUdpaTypedStruct() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder()
          .putFields("foo", com.google.protobuf.Value.newBuilder().setStringValue("bar").build())
          .build();

      TypedStruct udpaTypedStruct = TypedStruct.newBuilder()
          .setTypeUrl(FAKE_TYPE_URL)
          .setValue(struct)
          .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("child")
              .setTypedConfig(Any.pack(udpaTypedStruct))
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();
      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).isNull();
      verify(fakeProvider).parseFilterConfig(eq(struct), any());
    }

    @Test
    public void parseFilterConfig_withXdsTypedStruct() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder()
          .putFields("foo", com.google.protobuf.Value.newBuilder().setStringValue("bar").build())
          .build();

      com.github.xds.type.v3.TypedStruct xdsTypedStruct =
          com.github.xds.type.v3.TypedStruct.newBuilder()
              .setTypeUrl(FAKE_TYPE_URL)
              .setValue(struct)
              .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("child")
              .setTypedConfig(Any.pack(xdsTypedStruct))
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();
      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).isNull();
      verify(fakeProvider).parseFilterConfig(eq(struct), any());
    }

    @Test
    public void parseFilterConfig_rejectsOverrideMessage() {
      ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(Matcher.newBuilder().build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(overrideProto), getFilterContext());

      assertThat(result.errorDetail).contains("Expected ExtensionWithMatcher but got");
    }

    @Test
    public void parseFilterConfigOverride_rejectsConfigMessage() {
      ExtensionWithMatcher configProto = createExtensionWithMatcher(Matcher.newBuilder().build());

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(Any.pack(configProto), getFilterContext());

      assertThat(result.errorDetail).contains("Expected ExtensionWithMatcherPerRoute but got");
    }

    @Test
    public void parseFilterConfig_missingXdsMatcherIsAllowedAndActsAsNoOp() {
      // Unlike the override, a top-level config without a matcher is valid; the filter becomes a
      // passthrough and a per-route override may still supply a matcher.
      ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
              .setName("composite")
              .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
      assertThat(result.config.matcher).isNull();

      CompositeFilter filter = newFilter("composite");
      assertThat(filter.buildClientInterceptor(
          result.config, null, mock(ScheduledExecutorService.class))).isNull();
      assertThat(filter.buildServerInterceptor(result.config, null)).isNull();
    }

    @Test
    public void whenEnabled_reportedAsSupportedOnBothSides() {
      assertThat(provider.isClientFilter()).isTrue();
      assertThat(provider.isServerFilter()).isTrue();
    }

    @Test
    public void clientInterceptor_delegatesToMatchedChild() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall childCall = mock(ClientCall.class);
      when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeClientInterceptor).interceptCall(any(), any(), any());
      verify(childCall).start(any(), eq(headers));
    }

    @Test
    public void clientInterceptor_noMatchFailsWithUnavailable() {
      Matcher matcher = Matcher.newBuilder().build();
      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      Metadata headers = new Metadata();
      call.start(listener, headers);

      verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
      verify(next, never()).newCall(any(), any());

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(statusCaptor.getValue().getDescription())
          .contains("no match found in composite filter");
    }

    @Test
    public void clientInterceptor_delegatesChain() {
      TypedExtensionConfig child1 = TypedExtensionConfig.newBuilder()
          .setName("child1")
          .setTypedConfig(Any.newBuilder()
              .setTypeUrl(FAKE_TYPE_URL)
              .setValue(Composite.getDefaultInstance().toByteString())
              .build())
          .build();
      TypedExtensionConfig child2 = TypedExtensionConfig.newBuilder()
          .setName("child2")
          .setTypedConfig(Any.newBuilder()
              .setTypeUrl(FAKE_TYPE_URL)
              .setValue(Composite.getDefaultInstance().toByteString())
              .build())
          .build();

      FilterChainConfiguration filterChain = FilterChainConfiguration.newBuilder()
          .addTypedConfig(child1)
          .addTypedConfig(child2)
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_chain")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setFilterChain(filterChain)
                      .build().toByteString())
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      // Nested interceptors are built when the composite interceptor is built, so stub before that.
      fakeClientInterceptor = mock(ClientInterceptor.class);
      when(fakeFilter.buildClientInterceptor(any(), any(), any()))
          .thenReturn(fakeClientInterceptor);

      org.mockito.Mockito.doAnswer(invocation -> {
        Channel nextArg = (Channel) invocation.getArguments()[2];
        return nextArg.newCall(
            (MethodDescriptor<?, ?>) invocation.getArguments()[0],
            (CallOptions) invocation.getArguments()[1]);
      }).when(fakeClientInterceptor).interceptCall(any(), any(), any());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall nextCall = mock(ClientCall.class);
      when(next.newCall(any(), any())).thenReturn(nextCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeFilter, times(2)).buildClientInterceptor(any(), any(), any());
      verify(fakeClientInterceptor, times(2)).interceptCall(any(), any(), any());
    }

    @Test
    public void clientInterceptor_samplePercentAlwaysFalse_skipsFilter() {
      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_sampled")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("child")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(FAKE_TYPE_URL)
                              .setValue(Composite.getDefaultInstance().toByteString())
                              .build())
                          .build())
                      .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                          .setDefaultValue(FractionalPercent.newBuilder()
                              .setNumerator(0)
                              .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall nextCall = mock(ClientCall.class);
      when(next.newCall(any(), any())).thenReturn(nextCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);
      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
      verify(next).newCall(any(), any());
      verify(nextCall).start(any(), eq(headers));
    }

    @Test
    public void clientInterceptor_skipsOnSkipFilter() {
      Any skipActionAny = Any.newBuilder()
          .setTypeUrl("type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3"
              + ".SkipFilter")
          .setValue(com.google.protobuf.ByteString.EMPTY)
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_skip")
              .setTypedConfig(skipActionAny)
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall nextCall = mock(ClientCall.class);
      when(next.newCall(any(), any())).thenReturn(nextCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
      verify(next).newCall(any(), any());
      verify(nextCall).start(any(), eq(headers));
    }

    @Test
    public void clientInterceptor_nestedFilterOutlivesRpc() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall childCall = mock(ClientCall.class);
      when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ClientCall.Listener responseListener = mock(ClientCall.Listener.class);
      call.start(responseListener, headers);

      ArgumentCaptor<ClientCall.Listener> listenerCaptor =
          ArgumentCaptor.forClass(ClientCall.Listener.class);
      verify(childCall).start(listenerCaptor.capture(), eq(headers));

      ClientCall.Listener capturedListener = listenerCaptor.getValue();
      capturedListener.onClose(Status.OK, new Metadata());

      // Closing the RPC must NOT close the nested filter; it is shared across RPCs.
      verify(fakeFilter, never()).close();
    }

    @Test
    public void clientInterceptor_unsupportedSideChildFilterFailsWithUnavailable() {
      Matcher.OnMatch matchAction = createExecuteAction("unsupported", FAKE_UNSUPPORTED_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      call.start(listener, headers);

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(statusCaptor.getValue().getDescription())
          .contains("not supported on client side");
    }

    @Test
    public void clientInterceptor_callMethodsThrowBeforeStart() {
      CompositeFilter filter = newFilter("composite");
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      try {
        call.request(1);
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        assertThat(e.getMessage()).isEqualTo("Not started");
      }

      try {
        call.halfClose();
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        assertThat(e.getMessage()).isEqualTo("Not started");
      }

      try {
        call.sendMessage(null);
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        assertThat(e.getMessage()).isEqualTo("Not started");
      }

      try {
        call.setMessageCompression(true);
        fail("Expected IllegalStateException");
      } catch (IllegalStateException e) {
        assertThat(e.getMessage()).isEqualTo("Not started");
      }

      // Cancel before start is allowed and records cancel status
      call.cancel("cancelled before start", null);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      call.start(listener, new Metadata());

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);
      assertThat(statusCaptor.getValue().getDescription()).contains("cancelled before start");
    }

    /**
     * Blocks inside {@code newCall} so the test can act while {@code start()} is between
     * "started" and "delegate installed". A real channel's {@code newCall} can block on name
     * resolution or an LB pick, so this window is not artificial.
     */
    private static final class BlockingChannel extends Channel {
      final CountDownLatch inNewCall = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      private final ClientCall<?, ?> call;

      BlockingChannel(ClientCall<?, ?> call) {
        this.call = call;
      }

      @SuppressWarnings("unchecked")
      @Override
      public <R, P> ClientCall<R, P> newCall(
          MethodDescriptor<R, P> methodDescriptor, CallOptions callOptions) {
        inNewCall.countDown();
        try {
          assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return (ClientCall<R, P>) call;
      }

      @Override
      public String authority() {
        return "test-authority";
      }
    }

    @Test
    public void clientInterceptor_requestDuringStartIsNotDropped() throws Exception {
      CompositeFilter filter = newFilter("composite");
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      when(mockMatcher.match(any())).thenReturn(
          io.grpc.xds.internal.matcher.MatchResult.create(Collections.emptyList()));
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
          mock(ScheduledExecutorService.class));

      ClientCall nextCall = mock(ClientCall.class);
      BlockingChannel next = new BlockingChannel(nextCall);
      final ClientCall<Void, Void> call =
          interceptor.interceptCall(createMockMethod(), CallOptions.DEFAULT, next);
      final Metadata headers = new Metadata();

      Thread starter = new Thread(() -> call.start(mock(ClientCall.Listener.class), headers));
      starter.start();
      assertThat(next.inNewCall.await(5, TimeUnit.SECONDS)).isTrue();

      // ClientCall permits request() from any thread, so this is legal even though start() has
      // not returned. Before the fix the demand went to the no-op call and the RPC hung.
      call.request(5);

      next.release.countDown();
      starter.join(5000);

      verify(nextCall).start(any(), eq(headers));
      verify(nextCall).request(5);
    }

    @Test
    public void clientInterceptor_cancelDuringStartClosesListenerWithoutStartingCall()
        throws Exception {
      CompositeFilter filter = newFilter("composite");
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      when(mockMatcher.match(any())).thenReturn(
          io.grpc.xds.internal.matcher.MatchResult.create(Collections.emptyList()));
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
          mock(ScheduledExecutorService.class));

      ClientCall nextCall = mock(ClientCall.class);
      BlockingChannel next = new BlockingChannel(nextCall);
      final ClientCall<Void, Void> call =
          interceptor.interceptCall(createMockMethod(), CallOptions.DEFAULT, next);
      final ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      final AtomicReference<Throwable> startFailure = new AtomicReference<>();

      Thread starter = new Thread(() -> {
        try {
          call.start(listener, new Metadata());
        } catch (Throwable t) {
          startFailure.set(t);
        }
      });
      starter.start();
      assertThat(next.inNewCall.await(5, TimeUnit.SECONDS)).isTrue();

      // cancel() is the one method explicitly allowed before start() returns.
      call.cancel("cancelled mid start", null);

      next.release.countDown();
      starter.join(5000);

      assertThat(startFailure.get()).isNull();
      // The downstream call was never started, so no stream and no request headers were created.
      verify(nextCall, never()).start(any(), any());
      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);
      assertThat(statusCaptor.getValue().getDescription()).contains("cancelled mid start");
    }

    @Test
    public void clientInterceptor_cancelIsNeverDeliveredConcurrentlyWithDownstreamStart()
        throws Exception {
      CompositeFilter filter = newFilter("composite");
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      when(mockMatcher.match(any())).thenReturn(
          io.grpc.xds.internal.matcher.MatchResult.create(Collections.emptyList()));
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
          mock(ScheduledExecutorService.class));

      final CountDownLatch inDownstreamStart = new CountDownLatch(1);
      final CountDownLatch releaseDownstreamStart = new CountDownLatch(1);
      final AtomicBoolean startInProgress = new AtomicBoolean();
      final AtomicBoolean cancelRacedStart = new AtomicBoolean();
      final AtomicBoolean cancelDelivered = new AtomicBoolean();
      final ClientCall<Void, Void> downstream = new ClientCall<Void, Void>() {
        @Override
        public void start(ClientCall.Listener<Void> responseListener, Metadata headers) {
          startInProgress.set(true);
          inDownstreamStart.countDown();
          try {
            releaseDownstreamStart.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          startInProgress.set(false);
        }

        @Override
        public void cancel(String message, Throwable cause) {
          cancelDelivered.set(true);
          if (startInProgress.get()) {
            cancelRacedStart.set(true);
          }
        }

        @Override
        public void request(int numMessages) {}

        @Override
        public void halfClose() {}

        @Override
        public void sendMessage(Void message) {}
      };
      Channel next = new Channel() {
        @SuppressWarnings("unchecked")
        @Override
        public <R, P> ClientCall<R, P> newCall(
            MethodDescriptor<R, P> methodDescriptor, CallOptions callOptions) {
          return (ClientCall<R, P>) downstream;
        }

        @Override
        public String authority() {
          return "test-authority";
        }
      };

      final ClientCall<Void, Void> call =
          interceptor.interceptCall(createMockMethod(), CallOptions.DEFAULT, next);
      Thread starter = new Thread(
          () -> call.start(mock(ClientCall.Listener.class), new Metadata()));
      starter.start();
      assertThat(inDownstreamStart.await(5, TimeUnit.SECONDS)).isTrue();

      // The downstream call is mid-start. ClientCall is not thread-safe, so cancel() must not be
      // handed to it now; the fix keeps delegate unpublished until start() has returned.
      call.cancel("cancelled mid start", null);
      releaseDownstreamStart.countDown();
      starter.join(5000);

      assertThat(cancelRacedStart.get()).isFalse();
      assertThat(cancelDelivered.get()).isTrue();
    }

    @Test
    public void clientInterceptor_usesOverrideMatcher_overrideMatches() {
      Matcher.OnMatch baseAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher baseMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", baseAction))
              .build())
          .build();
      ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
          provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

      Any skipActionAny = Any.newBuilder()
          .setTypeUrl("type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3"
              + ".SkipFilter")
          .setValue(com.google.protobuf.ByteString.EMPTY)
          .build();
      Matcher.OnMatch overrideAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("override_skip")
              .setTypedConfig(skipActionAny)
              .build())
          .build();
      Matcher overrideMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", overrideAction))
              .build())
          .build();
      ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(overrideMatcher)
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
          provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall nextCall = mock(ClientCall.class);
      when(next.newCall(any(), any())).thenReturn(nextCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
      verify(next).newCall(any(), any());
      verify(nextCall).start(any(), eq(headers));
    }

    @Test
    public void clientInterceptor_usesOverrideMatcher_overrideNoMatchFails() {
      Matcher.OnMatch baseAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher baseMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", baseAction))
              .build())
          .build();
      ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
          provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

      // Override matcher has no match rules
      Matcher overrideMatcher = Matcher.newBuilder().build();
      ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(overrideMatcher)
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
          provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
      call.start(listener, headers);

      verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
      verify(next, never()).newCall(any(), any());
      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    @Test
    public void serverInterceptor_delegatesToMatchedChild() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

      ServerCall call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call.getMethodDescriptor()).thenReturn(createMockMethod());

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ServerCallHandler next = mock(ServerCallHandler.class);
      ServerCall.Listener listener = mock(ServerCall.Listener.class);
      when(next.startCall(any(), any())).thenReturn(listener);

      interceptor.interceptCall(call, headers, next);

      verify(fakeServerInterceptor).interceptCall(eq(call), eq(headers), any());
    }

    @Test
    public void serverInterceptor_noMatchFailsWithUnavailable() {
      Matcher matcher = Matcher.newBuilder().build();
      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

      ServerCall call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call.getMethodDescriptor()).thenReturn(createMockMethod());

      ServerCallHandler next = mock(ServerCallHandler.class);
      Metadata headers = new Metadata();

      interceptor.interceptCall(call, headers, next);

      verify(fakeServerInterceptor, never()).interceptCall(any(), any(), any());
      verify(next, never()).startCall(any(), any());

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(call).close(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(statusCaptor.getValue().getDescription())
          .contains("no match found in composite filter");
    }

    @Test
    public void serverInterceptor_unsupportedSideChildFilterFailsWithUnavailable() {
      Matcher.OnMatch matchAction = createExecuteAction("unsupported", FAKE_UNSUPPORTED_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

      ServerCall call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call.getMethodDescriptor()).thenReturn(createMockMethod());

      ServerCallHandler next = mock(ServerCallHandler.class);
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      interceptor.interceptCall(call, headers, next);

      verify(fakeServerInterceptor, never()).interceptCall(any(), any(), any());
      verify(next, never()).startCall(any(), any());

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(call).close(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
      assertThat(statusCaptor.getValue().getDescription())
          .contains("not supported on server side");
    }

    @Test
    public void serverInterceptor_nestedFilterOutlivesRpcs() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

      ServerCall call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call.getMethodDescriptor()).thenReturn(createMockMethod());

      ServerCall.Listener innerListener = mock(ServerCall.Listener.class);
      when(fakeServerInterceptor.interceptCall(any(), any(), any())).thenAnswer(invocation -> {
        ServerCallHandler handler = invocation.getArgument(2);
        return handler.startCall((ServerCall) invocation.getArgument(0),
            (Metadata) invocation.getArgument(1));
      });

      ServerCallHandler next = mock(ServerCallHandler.class);
      when(next.startCall(any(), any())).thenReturn(innerListener);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ServerCall.Listener listener = interceptor.interceptCall(call, headers, next);

      listener.onComplete();
      verify(fakeFilter, never()).close();

      listener.onCancel();
      verify(fakeFilter, never()).close();

      // A second RPC reuses the same nested filter instance rather than creating a new one.
      ServerCall call2 = mock(ServerCall.class);
      when(call2.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call2.getMethodDescriptor()).thenReturn(createMockMethod());
      ServerCall.Listener listener2 = interceptor.interceptCall(call2, headers, next);
      listener2.onCancel();
      verify(fakeFilter, never()).close();
      verify(fakeProvider, times(1)).newInstance(any());
    }

    /** Parses a top-level composite config whose single action runs the named nested filter. */
    private CompositeFilter.CompositeFilterConfig configWithChild(String childName) {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar",
                  createExecuteAction(childName, FAKE_TYPE_URL)))
              .build())
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(result.errorDetail).isNull();
      return result.config;
    }

    @Test
    public void nestedFilter_resolvedOncePerKeyAcrossRoutes() {
      CompositeFilter filter = newFilter("composite");
      ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
      // The resolver hands the same config object to every route of a single LDS update.
      CompositeFilter.CompositeFilterConfig config = configWithChild("child");

      filter.buildClientInterceptor(config, null, scheduler);
      filter.buildClientInterceptor(config, null, scheduler);
      filter.buildClientInterceptor(config, null, scheduler);

      // One instance for the one nested name, created by the framework's map, not by the
      // composite filter.
      verify(fakeProvider, times(1)).newInstance(any());
      verify(fakeFilter, never()).close();
    }

    @Test
    public void nestedFilter_routesSelectingDifferentChildrenDoNotEvictEachOther() {
      CompositeFilter filter = newFilter("composite");
      ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

      // A single LDS update: the resolver hands the same top-level config object to every route,
      // but individual routes may carry ExtensionWithMatcherPerRoute overrides that select
      // different nested filters.
      CompositeFilter.CompositeFilterConfig topLevel = configWithChild("child_a");
      CompositeFilter.CompositeFilterConfig routeOverride = configWithChild("child_b");

      filter.buildClientInterceptor(topLevel, null, scheduler);           // route 1 -> child_a
      filter.buildClientInterceptor(topLevel, routeOverride, scheduler);  // route 2 -> child_b
      filter.buildClientInterceptor(topLevel, null, scheduler);           // route 3 -> child_a

      verify(fakeProvider, times(2)).newInstance(any());
      verify(fakeFilter, never()).close();
    }

    @Test
    public void nestedFilter_reusedAcrossConfigUpdates() {
      CompositeFilter filter = newFilter("composite");
      ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

      // Two separate LDS updates that both still configure "child".
      filter.buildClientInterceptor(configWithChild("child"), null, scheduler);
      filter.buildClientInterceptor(configWithChild("child"), null, scheduler);

      // The instance is reused, so the state it owns (connection pools, credential caches)
      // survives the update instead of being rebuilt. Retiring it is the framework's job: the
      // composite filter never closes a nested filter.
      verify(fakeProvider, times(1)).newInstance(any());
      verify(fakeFilter, never()).close();
    }

    @Test
    public void samplePercentDeterministic() {
      FractionalPercent percent = FractionalPercent.newBuilder()
          .setNumerator(50)
          .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
          .build();

      ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
      CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
          Collections.emptyList(), percent, mockRandom);

      when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
      assertThat(delegate.shouldExecute()).isTrue();

      when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
      assertThat(delegate.shouldExecute()).isFalse();
    }

    @Test
    public void samplePercentTenThousand() {
      FractionalPercent percent = FractionalPercent.newBuilder()
          .setNumerator(5000)
          .setDenominator(FractionalPercent.DenominatorType.TEN_THOUSAND)
          .build();

      ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
      CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
          Collections.emptyList(), percent, mockRandom);

      when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
      assertThat(delegate.shouldExecute()).isTrue();

      when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
      assertThat(delegate.shouldExecute()).isFalse();
    }

    @Test
    public void samplePercentMillion() {
      FractionalPercent percent = FractionalPercent.newBuilder()
          .setNumerator(500000)
          .setDenominator(FractionalPercent.DenominatorType.MILLION)
          .build();

      ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
      CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
          Collections.emptyList(), percent, mockRandom);

      when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
      assertThat(delegate.shouldExecute()).isTrue();

      when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
      assertThat(delegate.shouldExecute()).isFalse();
    }

    @Test
    public void samplePercentUnknownDenominator_rejected() {
      FractionalPercent percent = FractionalPercent.newBuilder()
          .setNumerator(50)
          .setDenominatorValue(9999) // a denominator this build does not know
          .build();

      // Falling back to HUNDRED would read a numerator meant as a fraction of some much larger
      // denominator as a percentage, running the action far more often than configured.
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> new CompositeFilter.FilterDelegate(
              Collections.emptyList(), percent, mock(ThreadSafeRandom.class)));
      assertThat(e).hasMessageThat().contains("Unknown denominator type");
    }

    @Test
    public void parseFilterConfig_unknownSamplePercentDenominator_nacks() {
      Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_sampled")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                          .setDefaultValue(FractionalPercent.newBuilder()
                              .setNumerator(50)
                              .setDenominatorValue(9999)
                              .build())
                          .build())
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("child")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(FAKE_TYPE_URL)
                              .setValue(Composite.getDefaultInstance().toByteString())
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", action))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains("Unknown denominator type");
    }

    @Test
    public void parseFilterConfig_samplePercentWithoutDefaultValue_nacks() {
      // A103 requires default_value whenever sample_percent is set. An unset message would read
      // as 0%, silently disabling the action, so it has to be rejected instead.
      Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_sampled")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                      + ".ExecuteFilterAction")
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                          .setRuntimeKey("ignored_by_grpc")
                          .build())
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("child")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(FAKE_TYPE_URL)
                              .setValue(Composite.getDefaultInstance().toByteString())
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", action))
              .build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail)
          .contains("ExecuteFilterAction.sample_percent.default_value: field not set");
    }

    @Test
    public void filterConfigParseContext_recursionDepthDefaultsToZero() {
      // Callers outside the composite filter never set a depth, so the default has to be the
      // top-level value rather than an absent one every reader has to translate.
      assertThat(getFilterContext().recursionDepth()).isEqualTo(0);
    }

    @Test
    public void filterRegistry_compositeFilterRegistered() {
      FilterRegistry registry = FilterRegistry.getDefaultRegistry();
      Filter.Provider provider1 = registry.get(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER);
      assertThat(provider1).isInstanceOf(CompositeFilter.Provider.class);
      Filter.Provider provider2 =
          registry.get(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE);
      assertThat(provider2).isInstanceOf(CompositeFilter.Provider.class);
    }

    @Test
    public void clientInterceptor_streamingCall_delegatesAllMethods() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall childCall = mock(ClientCall.class);
      when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ClientCall.Listener responseListener = mock(ClientCall.Listener.class);
      call.start(responseListener, headers);

      ArgumentCaptor<ClientCall.Listener> listenerCaptor =
          ArgumentCaptor.forClass(ClientCall.Listener.class);
      verify(childCall).start(listenerCaptor.capture(), eq(headers));

      // Test streaming call methods delegation
      call.request(5);
      verify(childCall).request(5);

      call.sendMessage(null);
      verify(childCall).sendMessage(null);

      call.setMessageCompression(true);
      verify(childCall).setMessageCompression(true);

      call.halfClose();
      verify(childCall).halfClose();

      // Test response listener delegation
      ClientCall.Listener capturedListener = listenerCaptor.getValue();
      Metadata respHeaders = new Metadata();
      capturedListener.onHeaders(respHeaders);
      verify(responseListener).onHeaders(respHeaders);

      capturedListener.onMessage(null);
      verify(responseListener).onMessage(null);

      Metadata trailers = new Metadata();
      capturedListener.onClose(Status.OK, trailers);
      verify(responseListener).onClose(Status.OK, trailers);

      // The nested filter must survive the RPC; it is shared across RPCs.
      verify(fakeFilter, never()).close();
    }

    @Test
    public void clientInterceptor_unaryCall_methodsSafeAfterNoMatch() {
      Matcher matcher = Matcher.newBuilder().build();
      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      Metadata headers = new Metadata();
      call.start(listener, headers);

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

      // Stub call sequence after start() must not throw IllegalStateException("Not started")
      call.request(1);
      call.sendMessage(null);
      call.halfClose();
      call.cancel("cancel", null);
    }

    @Test
    public void clientInterceptor_unaryCall_methodsSafeAfterUnsupportedSideChildFilter() {
      Matcher.OnMatch matchAction = createExecuteAction("unsupported", FAKE_UNSUPPORTED_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
      call.start(listener, headers);

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

      // Subsequent calls from stubs must be safe no-ops
      call.request(1);
      call.sendMessage(null);
      call.halfClose();
      call.cancel("cancel", null);
    }

    @Test
    public void clientInterceptor_unaryCall_methodsSafeAfterCancelledBeforeStart() {
      CompositeFilter filter = newFilter("composite");
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      call.cancel("cancelled before start", null);

      ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
      call.start(listener, new Metadata());

      ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
      verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
      assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);

      // Subsequent calls from stubs must be safe no-ops
      call.request(1);
      call.sendMessage(null);
      call.halfClose();
    }

    @Test
    public void filterContext_passesFilterNameAndMetricsRecorderToChild() {
      Matcher.OnMatch matchAction = createExecuteAction("my_child_filter", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      MetricRecorder expectedRecorder = mock(MetricRecorder.class);
      List<String> lookedUpKeys = new ArrayList<>();
      CompositeFilter filter = (CompositeFilter) provider.newInstance(
          FilterContext.create(
              "composite",
              expectedRecorder,
              key -> {
                lookedUpKeys.add(key);
                return fakeFilter;
              }));
      ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
          mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      when(fakeClientInterceptor.interceptCall(any(), any(), any()))
          .thenReturn(mock(ClientCall.class));

      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
      call.start(mock(ClientCall.Listener.class), headers);

      assertThat(lookedUpKeys).containsExactly("my_child_filter_" + FAKE_TYPE_URL);
    }

    @Test
    public void serverInterceptor_streamingCall_delegatesAllMethods() {
      Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
              .build())
          .build();

      ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

      ServerCall call = mock(ServerCall.class);
      when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(call.getMethodDescriptor()).thenReturn(createMockMethod());

      ServerCall.Listener childListener = mock(ServerCall.Listener.class);
      when(fakeServerInterceptor.interceptCall(any(), any(), any())).thenReturn(childListener);

      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

      ServerCallHandler next = mock(ServerCallHandler.class);
      ServerCall.Listener listener = interceptor.interceptCall(call, headers, next);

      // Verify streaming message delegation
      listener.onMessage(null);
      verify(childListener).onMessage(null);

      listener.onHalfClose();
      verify(childListener).onHalfClose();

      listener.onReady();
      verify(childListener).onReady();

      // Completing the RPC must NOT tear down the nested filter: nested filters are long-lived
      // and shared across RPCs, just like top-level filters.
      listener.onComplete();
      verify(childListener).onComplete();
      verify(fakeFilter, never()).close();
    }

    @Test
    public void matchContext_pathAndAuthorityCapturedOnClientAndServer() {
      UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
      when(mockMatcher.match(any())).thenReturn(
          io.grpc.xds.internal.matcher.MatchResult.noMatch(Collections.emptyList()));

      CompositeFilter.CompositeFilterConfig config =
          new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap());

      CompositeFilter filter = newFilter("composite");

      // Client side verification
      ClientInterceptor clientInterceptor = filter.buildClientInterceptor(
          config, null, mock(ScheduledExecutorService.class));
      Channel next = mock(Channel.class);
      when(next.authority()).thenReturn("my-channel-authority:443");

      // fullMethodName is "service/method"
      MethodDescriptor<Void, Void> method = createMockMethod();
      ClientCall<Void, Void> clientCall =
          clientInterceptor.interceptCall(method, CallOptions.DEFAULT, next);

      Metadata clientHeaders = new Metadata();
      clientCall.start(mock(ClientCall.Listener.class), clientHeaders);

      ArgumentCaptor<io.grpc.xds.internal.matcher.MatchContext> clientContextCaptor =
          ArgumentCaptor.forClass(io.grpc.xds.internal.matcher.MatchContext.class);
      verify(mockMatcher).match(clientContextCaptor.capture());

      io.grpc.xds.internal.matcher.MatchContext clientContext = clientContextCaptor.getValue();
      assertThat(clientContext.getPath()).isEqualTo("/service/method");
      assertThat(clientContext.getHost()).isEqualTo("my-channel-authority:443");
      // ":method" is the HTTP method, not the gRPC method name; gRPC always uses POST.
      assertThat(clientContext.getMethod()).isEqualTo("POST");

      // Server side verification
      ServerInterceptor serverInterceptor = filter.buildServerInterceptor(config, null);
      ServerCall serverCall = mock(ServerCall.class);
      when(serverCall.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
      when(serverCall.getMethodDescriptor()).thenReturn(createMockMethod());
      when(serverCall.getAuthority()).thenReturn("my-server-authority:50051");

      Metadata serverHeaders = new Metadata();
      serverInterceptor.interceptCall(serverCall, serverHeaders, mock(ServerCallHandler.class));

      ArgumentCaptor<io.grpc.xds.internal.matcher.MatchContext> serverContextCaptor =
          ArgumentCaptor.forClass(io.grpc.xds.internal.matcher.MatchContext.class);
      verify(mockMatcher, times(2)).match(serverContextCaptor.capture());

      io.grpc.xds.internal.matcher.MatchContext serverContext = serverContextCaptor.getValue();
      assertThat(serverContext.getPath()).isEqualTo("/service/method");
      assertThat(serverContext.getHost()).isEqualTo("my-server-authority:50051");
      // ":method" is the HTTP method, not the gRPC method name; gRPC always uses POST.
      assertThat(serverContext.getMethod()).isEqualTo("POST");
    }

    @Test
    public void endToEnd_routeOverride_withExtensionWithMatcherPerRoute() {
      // Base composite filter matches "header_env=prod" -> executes child filter
      Matcher.OnMatch baseAction = createExecuteAction("prod_child", FAKE_TYPE_URL);
      Matcher baseMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("header_env", "prod", baseAction))
              .build())
          .build();
      ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
      ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
          provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

      // Override composite filter matches "header_env=staging" -> executes child filter
      Matcher.OnMatch overrideAction = createExecuteAction("staging_child", FAKE_TYPE_URL);
      Matcher overrideMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("header_env", "staging", overrideAction))
              .build())
          .build();
      ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(overrideMatcher)
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
          provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

      CompositeFilter filter = newFilter("composite");
      // Build interceptor with base config overridden by route config
      ClientInterceptor interceptor = filter.buildClientInterceptor(
          baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

      Channel next = mock(Channel.class);
      ClientCall childCall = mock(ClientCall.class);
      when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

      MethodDescriptor<Void, Void> method = createMockMethod();

      // Call with header_env=staging should match the override matcher
      ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);
      Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("header_env", Metadata.ASCII_STRING_MARSHALLER), "staging");
      call.start(mock(ClientCall.Listener.class), headers);

      verify(fakeClientInterceptor).interceptCall(any(), any(), any());
      verify(childCall).start(any(), eq(headers));
    }

    // =========================================================================
    // 1. RECURSION DEPTH BOUNDARY: Depth 7 (allowed) vs Depth 8 (rejected)
    // =========================================================================

    @Test
    public void recursionDepth_contextDepth7_allowed() {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("c", FAKE_TYPE_URL)))
              .build())
          .build();
      Any configAny = Any.pack(createExtensionWithMatcher(matcher));

      FilterConfigParseContext ctxDepth7 = getFilterContext().toBuilder()
          .recursionDepth(7)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(configAny, ctxDepth7);

      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
    }

    @Test
    public void recursionDepth_contextDepth8_rejected() {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("c", FAKE_TYPE_URL)))
              .build())
          .build();
      Any configAny = Any.pack(createExtensionWithMatcher(matcher));

      FilterConfigParseContext ctxDepth8 = getFilterContext().toBuilder()
          .recursionDepth(8)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(configAny, ctxDepth8);

      assertThat(result.errorDetail).contains("Maximum recursion depth of 8 exceeded");
      assertThat(result.config).isNull();
    }

    @Test
    public void recursionDepthOverride_contextDepth7_allowed() {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("c", FAKE_TYPE_URL)))
              .build())
          .build();
      Any configAny = Any.pack(ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(matcher)
          .build());

      FilterConfigParseContext ctxDepth7 = getFilterContext().toBuilder()
          .recursionDepth(7)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(configAny, ctxDepth7);

      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
    }

    @Test
    public void recursionDepthOverride_contextDepth8_rejected() {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("c", FAKE_TYPE_URL)))
              .build())
          .build();
      Any configAny = Any.pack(ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(matcher)
          .build());

      FilterConfigParseContext ctxDepth8 = getFilterContext().toBuilder()
          .recursionDepth(8)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(configAny, ctxDepth8);

      assertThat(result.errorDetail).contains("Maximum recursion depth of 8 exceeded");
      assertThat(result.config).isNull();
    }

    @Test
    public void recursionDepth_nestedCompositeFilters_depth7Allowed() {
      final Any leafConfig = Any.pack(createExtensionWithMatcher(Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("leaf", FAKE_TYPE_URL)))
              .build())
          .build()));

      when(fakeProvider.parseFilterConfig(any(), any()))
          .thenAnswer(invocation -> {
            FilterConfigParseContext context = invocation.getArgument(1);
            int depth = context.recursionDepth();
            if (depth < 7) {
              return provider.parseFilterConfig(leafConfig, context);
            }
            return ConfigOrError.fromConfig(fakeConfig);
          });

      ConfigOrError<? extends FilterConfig> result =
          provider.parseFilterConfig(leafConfig, getFilterContext());

      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
    }

    @Test
    public void recursionDepth_nestedCompositeFilters_depth8Rejected() {
      final Any leafConfig = Any.pack(createExtensionWithMatcher(Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "v", createExecuteAction("child", FAKE_TYPE_URL)))
              .build())
          .build()));

      when(fakeProvider.parseFilterConfig(any(), any()))
          .thenAnswer(invocation -> {
            FilterConfigParseContext context = invocation.getArgument(1);
            int depth = context.recursionDepth();
            if (depth <= 8) {
              return provider.parseFilterConfig(leafConfig, context);
            }
            return ConfigOrError.fromConfig(fakeConfig);
          });

      ConfigOrError<? extends FilterConfig> result =
          provider.parseFilterConfig(leafConfig, getFilterContext());

      assertThat(result.errorDetail).contains("Maximum recursion depth of 8 exceeded");
    }

    // =========================================================================
    // 2. ADVERSARIAL KEEP_MATCHING=TRUE: Nested inside exactMatchMap, prefixMatchMap, onNoMatch
    // =========================================================================

    @Test
    public void keepMatching_directInExactMatchMap_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setExactMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("key", badAction)))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_directInPrefixMatchMap_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setPrefixMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("prefix/", badAction)))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_directInOnNoMatch_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setOnNoMatch(badAction)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_deeplyNestedInsideExactMatchMap_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher innerMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setExactMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("innerKey", badAction)))
          .build();

      Matcher outerMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setExactMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("outerKey",
                      Matcher.OnMatch.newBuilder().setMatcher(innerMatcher).build())))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(outerMatcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_deeplyNestedInsidePrefixMatchMap_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher innerMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setPrefixMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("innerPrefix/", badAction)))
          .build();

      Matcher outerMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setPrefixMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("outerPrefix/",
                      Matcher.OnMatch.newBuilder().setMatcher(innerMatcher).build())))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(outerMatcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_deeplyNestedInsideOnNoMatch_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher innerMatcher = Matcher.newBuilder()
          .setOnNoMatch(badAction)
          .build();

      Matcher outerMatcher = Matcher.newBuilder()
          .setOnNoMatch(Matcher.OnMatch.newBuilder().setMatcher(innerMatcher).build())
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(outerMatcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    @Test
    public void keepMatching_mixedNesting_allLevelsDetectedAndRejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setKeepMatching(true)
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("act")
              .setTypedConfig(Any.newBuilder().setTypeUrl(EXECUTE_ACTION_TYPE_URL).build()))
          .build();

      Matcher level3 = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setPrefixMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("l3/", badAction)))
          .build();

      Matcher level2 = Matcher.newBuilder()
          .setOnNoMatch(Matcher.OnMatch.newBuilder().setMatcher(level3).build())
          .build();

      Matcher level1 = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setExactMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("l1", Matcher.OnMatch.newBuilder().setMatcher(level2).build())))
          .build();

      Matcher root = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                  .setOnMatch(Matcher.OnMatch.newBuilder().setMatcher(level1).build())))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(root)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "keep_matching is not permitted anywhere in the composite filter matcher tree");
    }

    // =========================================================================
    // 3. ADVERSARIAL ACTION TYPE URLS: Unrecognized, Malformed, or Fail-Open
    // =========================================================================

    @Test
    public void actionTypeUrl_completelyUnrecognized_rejected() {
      Matcher.OnMatch badAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_unknown")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/unknown.BogusAction")
                  .setValue(ByteString.EMPTY)
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(badAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).isNotNull();
      assertThat(result.errorDetail).contains("Expected ExecuteFilterAction or SkipFilter but got");
    }

    @Test
    public void actionTypeUrl_bareFilterDirectlyAsOnMatchAction_rejected() {
      com.github.xds.core.v3.TypedExtensionConfig bareAction =
          com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_bare")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl(FAKE_TYPE_URL) // Registered filter directly as action
                  .setValue(ByteString.EMPTY)
                  .build())
              .build();
      Matcher.OnMatch bareFilterAction = Matcher.OnMatch.newBuilder()
          .setAction(bareAction)
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(bareFilterAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      // Per gRFC A103: "The actions in this tree must be one of two types: SkipFilter or
      // ExecuteFilterAction". A filter config used directly as an action is neither, so it must be
      // rejected outright rather than silently dropped from the delegate map (which would fail open
      // at runtime, letting the RPC through unfiltered).
      assertThat(result.config).isNull();
      assertThat(result.errorDetail)
          .contains("Expected ExecuteFilterAction or SkipFilter but got: " + FAKE_TYPE_URL);
    }

    @Test
    public void actionTypeUrl_executeFilterAction_emptyConfig_rejected() {
      ExecuteFilterAction emptyAction = ExecuteFilterAction.newBuilder().build();
      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("empty_execute_action")
              .setTypedConfig(Any.pack(emptyAction))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      // Per gRFC A103: "It is an error if neither typed_config nor filter_chain are set."
      assertThat(result.config).isNull();
      assertThat(result.errorDetail)
          .contains("ExecuteFilterAction must specify either typed_config or filter_chain");
    }

    @Test
    public void actionTypeUrl_executeFilterAction_emptyFilterChain_acceptedAsNoOp() {
      ExecuteFilterAction emptyChainAction = ExecuteFilterAction.newBuilder()
          .setFilterChain(FilterChainConfiguration.newBuilder().build())
          .build();
      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("empty_chain_action")
              .setTypedConfig(Any.pack(emptyChainAction))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      // A103 makes it an error only if neither typed_config nor filter_chain is set. An empty
      // filter_chain is set, so the action is valid and simply runs no nested filters.
      assertThat(result.errorDetail).isNull();
      assertThat(result.config.delegates).hasSize(1);
      assertThat(result.config.delegates.values().iterator().next().delegates).isEmpty();
    }

    @Test
    public void actionTypeUrl_skipFilter_corruptedProtoBytes_rejected() {
      // SkipFilter has no fields, but the payload must still be well-formed protobuf; accepting
      // garbage here would silently turn a corrupt action into "skip the nested filters".
      Matcher.OnMatch corruptSkip = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("skip_corrupt")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl("type.googleapis.com/"
                      + "envoy.extensions.filters.common.matcher.action.v3.SkipFilter")
                  .setValue(ByteString.copyFrom(new byte[]{(byte) 0xff, (byte) 0xff}))
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(corruptSkip).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains("Could not parse SkipFilter action");
    }

    @Test
    public void actionTypeUrl_executeFilterAction_corruptedProtoBytes_rejected() {
      Matcher.OnMatch corruptedAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_corrupt")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl(EXECUTE_ACTION_TYPE_URL)
                  .setValue(ByteString.copyFrom(new byte[]{(byte) 0xff, (byte) 0xff}))
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(corruptedAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).isNotNull();
    }

    @Test
    public void actionTypeUrl_executeFilterAction_unregisteredChildFilter_rejected() {
      Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_unregistered")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl(EXECUTE_ACTION_TYPE_URL)
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("child")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl("type.googleapis.com/unregistered.Filter")
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(action).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail)
          .contains("Action filter not found: type.googleapis.com/unregistered.Filter");
    }

    @Test
    public void actionTypeUrl_executeFilterAction_childFilterFailsParsing_rejected() {
      Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_failing")
              .setTypedConfig(Any.newBuilder()
                  .setTypeUrl(EXECUTE_ACTION_TYPE_URL)
                  .setValue(ExecuteFilterAction.newBuilder()
                      .setTypedConfig(TypedExtensionConfig.newBuilder()
                          .setName("child")
                          .setTypedConfig(Any.newBuilder()
                              .setTypeUrl(FAKE_FAILING_PARSE_TYPE_URL)
                              .build())
                          .build())
                      .build().toByteString())
                  .build())
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(action).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains("Child filter config parsing failed intentionally");
    }

    // =========================================================================
    // 4. TERMINAL FILTER (RouterFilter) DISGUISED INSIDE TypedStruct
    // =========================================================================

    @Test
    public void terminalFilter_disguisedInsideUdpaTypedStruct_rejected() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder().build();
      TypedStruct udpaTypedStruct = TypedStruct.newBuilder()
          .setTypeUrl(RouterFilter.TYPE_URL)
          .setValue(struct)
          .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("disguised_router")
              .setTypedConfig(Any.pack(udpaTypedStruct))
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void terminalFilter_disguisedInsideXdsTypedStruct_rejected() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder().build();
      com.github.xds.type.v3.TypedStruct xdsTypedStruct =
          com.github.xds.type.v3.TypedStruct.newBuilder()
              .setTypeUrl(RouterFilter.TYPE_URL)
              .setValue(struct)
              .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("disguised_router")
              .setTypedConfig(Any.pack(xdsTypedStruct))
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void terminalFilter_disguisedInsideUdpaTypedStruct_inFilterChain_rejected() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder().build();
      TypedStruct udpaTypedStruct = TypedStruct.newBuilder()
          .setTypeUrl(RouterFilter.TYPE_URL)
          .setValue(struct)
          .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setFilterChain(FilterChainConfiguration.newBuilder()
              .addTypedConfig(TypedExtensionConfig.newBuilder()
                  .setName("valid_child")
                  .setTypedConfig(Any.newBuilder()
                      .setTypeUrl(FAKE_TYPE_URL)
                      .setValue(Composite.getDefaultInstance().toByteString())
                      .build())
                  .build())
              .addTypedConfig(TypedExtensionConfig.newBuilder()
                  .setName("hidden_router")
                  .setTypedConfig(Any.pack(udpaTypedStruct))
                  .build())
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void terminalFilter_disguisedInsideXdsTypedStruct_inFilterChain_rejected() {
      com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder().build();
      com.github.xds.type.v3.TypedStruct xdsTypedStruct =
          com.github.xds.type.v3.TypedStruct.newBuilder()
              .setTypeUrl(RouterFilter.TYPE_URL)
              .setValue(struct)
              .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setFilterChain(FilterChainConfiguration.newBuilder()
              .addTypedConfig(TypedExtensionConfig.newBuilder()
                  .setName("hidden_router")
                  .setTypedConfig(Any.pack(xdsTypedStruct))
                  .build())
              .build())
          .build();

      Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action")
              .setTypedConfig(Any.pack(action))
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void terminalFilter_childProviderReturnsRouterConfig_rejected() {
      when(fakeProvider.parseFilterConfig(any(com.google.protobuf.Message.class), any()))
          .thenReturn((ConfigOrError) ConfigOrError.fromConfig(RouterFilter.ROUTER_CONFIG));

      Matcher matcher = Matcher.newBuilder()
          .setOnNoMatch(createExecuteAction("child", FAKE_TYPE_URL))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains(
          "Nested filter cannot be a terminal filter");
    }

    @Test
    public void terminalFilter_corruptedUdpaTypedStructBytes_rejected() {
      Any badUdpaAny = Any.newBuilder()
          .setTypeUrl("type.googleapis.com/udpa.type.v1.TypedStruct")
          .setValue(ByteString.copyFrom(new byte[]{(byte) 0x80}))
          .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("bad_udpa")
              .setTypedConfig(badUdpaAny)
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setOnNoMatch(Matcher.OnMatch.newBuilder()
              .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                  .setName("action")
                  .setTypedConfig(Any.pack(action))))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains("Failed to unpack TypedStruct");
    }

    @Test
    public void terminalFilter_corruptedXdsTypedStructBytes_rejected() {
      Any badXdsAny = Any.newBuilder()
          .setTypeUrl("type.googleapis.com/xds.type.v3.TypedStruct")
          .setValue(ByteString.copyFrom(new byte[]{(byte) 0x80}))
          .build();

      ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("bad_xds")
              .setTypedConfig(badXdsAny)
              .build())
          .build();

      Matcher matcher = Matcher.newBuilder()
          .setOnNoMatch(Matcher.OnMatch.newBuilder()
              .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                  .setName("action")
                  .setTypedConfig(Any.pack(action))))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

      assertThat(result.errorDetail).contains("Failed to unpack TypedStruct");
    }

    // =========================================================================
    // 5. ENVELOPE VALIDATION: ExtensionWithMatcher & ExtensionWithMatcherPerRoute
    // =========================================================================

    @Test
    public void envelope_nonAnyRawMessage_rejected() {
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Composite.getDefaultInstance(), getFilterContext());
      assertThat(result.errorDetail).contains(
          "Invalid message type: "
              + "io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite");
    }

    @Test
    public void envelope_wrongTypeUrl_rejected() {
      Any wrongAny = Any.newBuilder()
          .setTypeUrl(RouterFilter.TYPE_URL)
          .setValue(ByteString.EMPTY)
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(wrongAny, getFilterContext());
      assertThat(result.errorDetail)
          .contains("Expected ExtensionWithMatcher but got: " + RouterFilter.TYPE_URL);
    }

    @Test
    public void envelope_corruptedAnyBytes_rejected() {
      Any corruptAny = Any.newBuilder()
          .setTypeUrl(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER)
          .setValue(ByteString.copyFrom(new byte[]{(byte) 0x80}))
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(corruptAny, getFilterContext());
      assertThat(result.errorDetail).contains("Invalid proto:");
    }

    @Test
    public void envelope_missingExtensionConfig_rejected() {
      ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
          .setXdsMatcher(Matcher.getDefaultInstance())
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());
      assertThat(result.errorDetail).contains(
          "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
    }

    @Test
    public void envelope_extensionConfigMissingTypedConfig_rejected() {
      ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder().setName("composite").build())
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());
      assertThat(result.errorDetail).contains(
          "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
    }

    @Test
    public void envelope_extensionConfigNotComposite_rejected() {
      ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder()
              .setName("composite")
              .setTypedConfig(Any.newBuilder().setTypeUrl(RouterFilter.TYPE_URL).build())
              .build())
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());
      assertThat(result.errorDetail).contains(
          "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
    }

    @Test
    public void envelopeOverride_nonAnyRawMessage_rejected() {
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(Composite.getDefaultInstance(), getFilterContext());
      assertThat(result.errorDetail).contains(
          "Invalid message type: "
              + "io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite");
    }

    @Test
    public void envelopeOverride_wrongTypeUrl_rejected() {
      Any wrongAny = Any.newBuilder()
          .setTypeUrl(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER)
          .setValue(ByteString.EMPTY)
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(wrongAny, getFilterContext());
      assertThat(result.errorDetail).contains("Expected ExtensionWithMatcherPerRoute but got");
    }

    @Test
    public void envelopeOverride_corruptedBytes_rejected() {
      Any corruptAny = Any.newBuilder()
          .setTypeUrl(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE)
          .setValue(ByteString.copyFrom(new byte[]{(byte) 0x80}))
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(corruptAny, getFilterContext());
      assertThat(result.errorDetail).contains("Invalid proto:");
    }

    @Test
    public void envelopeOverride_missingXdsMatcherIsAllowedAndActsAsNoOp() {
      // A103 requires xds_matcher to be validated the same way as the corresponding top-level
      // field, and parseFilterConfig_missingXdsMatcherIsAllowedAndActsAsNoOp pins that an absent
      // one is permitted there. The override replaces the top-level matcher, so an absent one
      // replaces it with nothing and the filter stops matching on this route.
      ExtensionWithMatcherPerRoute proto = ExtensionWithMatcherPerRoute.newBuilder().build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(Any.pack(proto), getFilterContext());
      assertThat(result.errorDetail).isNull();
      assertThat(result.config).isNotNull();
      assertThat(result.config.matcher).isNull();

      // A null matcher yields no interceptor on either side: a passthrough, not a crash.
      CompositeFilter filter = newFilter("composite");
      CompositeFilter.CompositeFilterConfig base =
          new CompositeFilter.CompositeFilterConfig(mock(UnifiedMatcher.class),
              Collections.emptyMap());
      assertThat(filter.buildClientInterceptor(
          base, result.config, mock(ScheduledExecutorService.class))).isNull();
      assertThat(filter.buildServerInterceptor(base, result.config)).isNull();
    }

    // =========================================================================
    // 6. COVERAGE GAP TESTS: MatcherTree collection, closeAll errors, equals/toString,
    //    resolveDelegates edge cases, NOOP_CALL methods, post-build cancel race, FINE logs
    // =========================================================================

    @Test
    public void collectDelegates_matcherTreeExactAndPrefixMapsAndNestedMatcher() {
      Matcher.OnMatch exactAction = createExecuteAction("exact_child", FAKE_TYPE_URL);
      Matcher.OnMatch nestedLeafAction = createExecuteAction("nested_child", FAKE_TYPE_URL);
      Matcher nestedMatcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("h2", "v2", nestedLeafAction)))
          .build();
      Matcher.OnMatch nestedOnMatch = Matcher.OnMatch.newBuilder()
          .setMatcher(nestedMatcher)
          .build();
      Matcher.OnMatch fallbackAction = createExecuteAction("fallback_child", FAKE_TYPE_URL);

      com.github.xds.core.v3.TypedExtensionConfig headerInput =
          com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("request_headers")
              .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                  .setHeaderName("h1")
                  .build()))
              .build();

      Matcher exactTreeMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setInput(headerInput)
              .setExactMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("exact_val", exactAction)
                  .putMap("nested_val", nestedOnMatch)))
          .setOnNoMatch(fallbackAction)
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> exactRes =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(exactTreeMatcher)), getFilterContext());
      assertThat(exactRes.errorDetail).isNull();
      assertThat(exactRes.config.delegates).hasSize(3);

      Matcher.OnMatch prefixAction = createExecuteAction("prefix_child", FAKE_TYPE_URL);
      Matcher prefixTreeMatcher = Matcher.newBuilder()
          .setMatcherTree(Matcher.MatcherTree.newBuilder()
              .setInput(headerInput)
              .setPrefixMatchMap(Matcher.MatcherTree.MatchMap.newBuilder()
                  .putMap("pre_", prefixAction)))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> prefixRes =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(prefixTreeMatcher)), getFilterContext());
      assertThat(prefixRes.errorDetail).isNull();
      assertThat(prefixRes.config.delegates).hasSize(1);
    }


    @Test
    public void compositeFilterConfig_equalsHashCodeToString() {
      Matcher matcherProto = Matcher.newBuilder()
          .setOnNoMatch(createExecuteAction("c1", FAKE_TYPE_URL))
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> parsed =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcherProto)), getFilterContext());

      CompositeFilter.CompositeFilterConfig cfg = parsed.config;
      assertThat(cfg.equals(cfg)).isTrue();
      Object nonConfig = new Object();
      assertThat(cfg.equals(nonConfig)).isFalse();
      assertThat(cfg.equals(null)).isFalse();
      assertThat(cfg.toString()).contains("matcher=set");
      assertThat(cfg.toString()).contains("delegates=1");

      UnifiedMatcher um = mock(UnifiedMatcher.class);
      CompositeFilter.CompositeFilterConfig handBuilt1 =
          new CompositeFilter.CompositeFilterConfig(um, null);
      CompositeFilter.CompositeFilterConfig handBuilt2 =
          new CompositeFilter.CompositeFilterConfig(um, Collections.emptyMap());
      CompositeFilter.CompositeFilterConfig handBuiltDifferent =
          new CompositeFilter.CompositeFilterConfig(mock(UnifiedMatcher.class), null);

      assertThat(handBuilt1.equals(handBuilt2)).isTrue();
      assertThat(handBuilt1.hashCode()).isEqualTo(handBuilt2.hashCode());
      assertThat(handBuilt1.equals(handBuiltDifferent)).isFalse();
      assertThat(handBuilt1.equals(cfg)).isFalse();
      assertThat(cfg.equals(handBuilt1)).isFalse();
      assertThat(handBuilt1.toString()).contains("matcher=none");
    }

    @Test
    public void resolveDelegates_nullOrUnmatchedOrEmptyActions_returnsEmptyList() {
      assertThat(CompositeFilter.resolveDelegates(null, Collections.emptyMap())).isEmpty();
      assertThat(CompositeFilter.resolveDelegates(
          io.grpc.xds.internal.matcher.MatchResult.noMatch(), Collections.emptyMap())).isEmpty();
      assertThat(CompositeFilter.resolveDelegates(
          io.grpc.xds.internal.matcher.MatchResult.create(Collections.emptyList()),
          Collections.emptyMap())).isEmpty();
    }

    @Test
    public void noopCallMethods_andPostBuildCancelRace_andNullChildInterceptor_andFineLogging()
        throws Exception {
      java.util.logging.Logger julLogger =
          java.util.logging.Logger.getLogger(CompositeFilter.class.getName());
      java.util.logging.Level oldLevel = julLogger.getLevel();
      julLogger.setLevel(java.util.logging.Level.FINE);
      try {
        // Child filter returning null interceptor (passthrough) on both client and server
        when(fakeFilter.buildClientInterceptor(any(), any(), any())).thenReturn(null);
        when(fakeFilter.buildServerInterceptor(any(), any())).thenReturn(null);

        Matcher matcher = Matcher.newBuilder()
            .setOnNoMatch(createExecuteAction("c1", FAKE_TYPE_URL))
            .build();
        ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
            provider.parseFilterConfig(
                Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

        CompositeFilter filter = newFilter("composite");
        ClientInterceptor clientInterceptor = filter.buildClientInterceptor(
            res.config, null, mock(ScheduledExecutorService.class));
        ServerInterceptor serverInterceptor = filter.buildServerInterceptor(res.config, null);

        // Server call with FINE logging and null child interceptor
        ServerCall<Void, Void> serverCall = mock(ServerCall.class);
        when(serverCall.getMethodDescriptor()).thenReturn(createMockMethod());
        ServerCallHandler<Void, Void> nextHandler = mock(ServerCallHandler.class);
        serverInterceptor.interceptCall(serverCall, new Metadata(), nextHandler);
        verify(nextHandler).startCall(eq(serverCall), any(Metadata.class));

        // Client call where cancel arrives mid-start (during next.newCall construction)
        java.util.concurrent.atomic.AtomicReference<ClientCall<Void, Void>> outerCallRef =
            new java.util.concurrent.atomic.AtomicReference<>();
        Channel nextChannel = new Channel() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
              MethodDescriptor<ReqT, RespT> m, CallOptions co) {
            outerCallRef.get().cancel("mid-start cancel", null);
            return mock(ClientCall.class);
          }

          @Override
          public String authority() {
            return "test-auth";
          }
        };

        ClientCall<Void, Void> call = clientInterceptor.interceptCall(
            createMockMethod(), CallOptions.DEFAULT, nextChannel);
        outerCallRef.set(call);
        ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
        call.start(listener, new Metadata());

        ArgumentCaptor<Status> statusCap = ArgumentCaptor.forClass(Status.class);
        verify(listener).onClose(statusCap.capture(), any(Metadata.class));
        assertThat(statusCap.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);

        // Exercise NOOP_CALL methods on the cancelled call
        call.request(2);
        call.sendMessage(null);
        call.halfClose();
        call.cancel("again", null);

        // Starting an already-started call throws IllegalStateException
        assertThrows(IllegalStateException.class, () -> call.start(listener, new Metadata()));

        // Exercise CallOptions authority override branch (callOptions.getAuthority() != null)
        Channel passthroughNext = new Channel() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
              MethodDescriptor<ReqT, RespT> m, CallOptions co) {
            return mock(ClientCall.class);
          }

          @Override
          public String authority() {
            return "default-auth";
          }
        };
        ClientCall<Void, Void> authCall = clientInterceptor.interceptCall(
            createMockMethod(), CallOptions.DEFAULT.withAuthority("override-auth"),
            passthroughNext);
        authCall.start(mock(ClientCall.Listener.class), new Metadata());

        // Exercise NOOP_CALL.start directly so 100% of methods/lines in CompositeFilter are covered
        java.lang.reflect.Field noopField = null;
        for (Class<?> inner : CompositeFilter.class.getDeclaredClasses()) {
          if (inner.getSimpleName().equals("CompositeClientCall")) {
            noopField = inner.getDeclaredField("NOOP_CALL");
            noopField.setAccessible(true);
            ClientCall<?, ?> noop = (ClientCall<?, ?>) noopField.get(null);
            noop.start(null, null);
            break;
          }
        }
      } finally {
        julLogger.setLevel(oldLevel);
      }
    }

    @Test
    public void parseFilterConfig_nestedFilterMissingName_rejected() {
      Matcher.OnMatch missingNameAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("execute_action")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setTypedConfig(TypedExtensionConfig.newBuilder()
                      .setName("")
                      .setTypedConfig(Any.newBuilder().setTypeUrl(FAKE_TYPE_URL)))
                  .build())))
          .build();
      Matcher matcher = Matcher.newBuilder().setOnNoMatch(missingNameAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(res.config).isNull();
      assertThat(res.errorDetail).contains("Nested filter is missing a name");
    }

    @Test
    public void parseFilterConfig_duplicateNameWithinFilterChain_rejected() {
      TypedExtensionConfig child = TypedExtensionConfig.newBuilder()
          .setName("dup_child")
          .setTypedConfig(Any.newBuilder().setTypeUrl(FAKE_TYPE_URL))
          .build();
      Matcher.OnMatch chainAction = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("execute_chain")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setFilterChain(
                      io.envoyproxy.envoy.extensions.filters.http.composite.v3
                          .FilterChainConfiguration.newBuilder()
                          .addTypedConfig(child)
                          .addTypedConfig(child))
                  .build())))
          .build();
      Matcher matcher = Matcher.newBuilder().setOnNoMatch(chainAction).build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(res.config).isNull();
      assertThat(res.errorDetail)
          .contains("ExecuteFilterAction.filter_chain contains duplicate filter name: dup_child");
    }

    @Test
    public void parseFilterConfig_conflictingConfigAcrossActions_rejected() {
      Matcher.OnMatch action1 = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_1")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setTypedConfig(TypedExtensionConfig.newBuilder()
                      .setName("shared_name")
                      .setTypedConfig(Any.newBuilder()
                          .setTypeUrl(FAKE_TYPE_URL)
                          .setValue(ByteString.copyFromUtf8("cfg-A"))))
                  .build())))
          .build();
      Matcher.OnMatch action2 = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_2")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setTypedConfig(TypedExtensionConfig.newBuilder()
                      .setName("shared_name")
                      .setTypedConfig(Any.newBuilder()
                          .setTypeUrl(FAKE_TYPE_URL)
                          .setValue(ByteString.copyFromUtf8("cfg-B"))))
                  .build())))
          .build();
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("h", "1", action1))
              .addMatchers(createHeaderFieldMatcher("h", "2", action2)))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(res.config).isNull();
      assertThat(res.errorDetail)
          .contains("Nested filter name shared_name is used by two different filter configs");
    }

    @Test
    public void parseFilterConfig_identicalConfigAcrossActions_allowedAndSharesInstance() {
      TypedExtensionConfig identicalChild = TypedExtensionConfig.newBuilder()
          .setName("shared_name")
          .setTypedConfig(Any.newBuilder()
              .setTypeUrl(FAKE_TYPE_URL)
              .setValue(ByteString.copyFromUtf8("same-cfg")))
          .build();
      Matcher.OnMatch action1 = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_1")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setTypedConfig(identicalChild)
                  .build())))
          .build();
      Matcher.OnMatch action2 = Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName("action_2")
              .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                  .setTypedConfig(identicalChild)
                  .build())))
          .build();
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher("h", "1", action1))
              .addMatchers(createHeaderFieldMatcher("h", "2", action2)))
          .build();

      ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(res.errorDetail).isNull();

      CompositeFilter filter = newFilter("composite");
      filter.buildClientInterceptor(res.config, null, mock(ScheduledExecutorService.class));
      // Only one underlying Filter instance should be created for the shared name.
      verify(fakeProvider, times(1)).newInstance(any(FilterContext.class));
    }

    @Test
    public void nestedFilterConfigs_exposesAllChildConfigsForFrameworkReconciliation() {
      Matcher matcher = Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(createHeaderFieldMatcher(
                  "h", "1", createExecuteAction("c1", FAKE_TYPE_URL)))
              .addMatchers(createHeaderFieldMatcher(
                  "h", "2", createExecuteAction("c2", FAKE_TYPE_URL))))
          .build();
      ConfigOrError<CompositeFilter.CompositeFilterConfig> res =
          provider.parseFilterConfig(
              Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
      assertThat(res.errorDetail).isNull();
      List<String> keys = new ArrayList<>();
      for (Filter.NamedFilterConfig nfc : res.config.nestedFilterConfigs()) {
        keys.add(nfc.filterStateKey());
      }
      assertThat(keys).containsExactly("c1_" + FAKE_TYPE_URL, "c2_" + FAKE_TYPE_URL);
    }

    @Test
    public void nestedFilter_missingFromActiveFiltersLookup_throwsNpe() {
      CompositeFilter unreconciled = (CompositeFilter) provider.newInstance(
          FilterContext.create("composite", mock(MetricRecorder.class), k -> null));
      NullPointerException thrown = assertThrows(NullPointerException.class,
          () -> unreconciled.buildClientInterceptor(
              configWithChild("missing_child"), null, mock(ScheduledExecutorService.class)));
      assertThat(thrown).hasMessageThat()
          .contains("nested filter missing_child_" + FAKE_TYPE_URL + " not reconciled");
    }
  }

  @RunWith(JUnit4.class)
  public static class ClientE2eTest {

    private static final String SERVER_HOST_NAME = "test-server";
    private static final String RDS_NAME = "route-config.googleapis.com";
    private static final String CLUSTER_NAME = "cluster0";
    private static final String COMPOSITE_FILTER_NAME = "composite-filter";
    private static final String HTTP_CONNECTION_MANAGER_TYPE_URL =
        "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3"
            + ".HttpConnectionManager";

    private static final Metadata.Key<String> MATCH_HEADER =
        Metadata.Key.of("x-composite-match", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> OTHER_HEADER =
        Metadata.Key.of("x-composite-other", Metadata.ASCII_STRING_MARSHALLER);

    /** The response {@link DataPlaneRule}'s echo server sends when an RPC gets through. */
    private static final SimpleResponse EXPECTED_RESPONSE = SimpleResponse.newBuilder()
        .setResponseMessage("Hi, xDS! Authority= " + SERVER_HOST_NAME)
        .build();

    @Rule(order = 0)
    public ControlPlaneRule controlPlane = new ControlPlaneRule();

    @Rule(order = 1)
    public DataPlaneRule dataPlane = new DataPlaneRule(controlPlane);

    /**
     * A103: "Support for the composite filter will be guarded by the
     * GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER environment variable."
     *
     * <p>This must be set before the JUnit rules run, because {@link DataPlaneRule} starts the xDS
     * server - and parses its LDS - during {@code starting()}.
     */
    @BeforeClass
    public static void enableCompositeFilter() {
      System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    }

    @AfterClass
    public static void disableCompositeFilter() {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "If the matcher tree does not find a match, the RPC will be failed with UNAVAILABLE
    // status."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void noMatch_rpcFailsWithUnavailable() {
      pushCompositeConfig(matcherOnHeader("run", executeAction("fault", faultAbort(Status.Code
          .PERMISSION_DENIED))));

      // The matcher keys off MATCH_HEADER; this RPC does not carry it, so nothing matches.
      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithoutHeaders());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "If the matcher tree finds a SkipFilter match, the filter will simply pass the RPC
    // through to the next filter (the one after the composite filter), without delegating to any
    // nested filters."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void skipFilterAction_rpcPassesThrough() {
      pushCompositeConfig(matcherOnHeader("run", skipAction()));

      assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "if the RPC *is* sampled, the RPC will be passed to the nested filter chain before
    // sent to the next filter".  sample_percent is unset here, and A103 says of it: "Optional; if
    // unset, the specified filter(s) are always executed."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void executeFilterAction_matched_nestedFilterRuns() {
      pushCompositeConfig(matcherOnHeader("run",
          executeAction("fault", faultAbort(Status.Code.PERMISSION_DENIED))));

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "for each RPC, a random number will be generated between 0 and 100, and if that number
    // is less than the specified threshold, the specified filter(s) will be executed."  At 0% no
    // random number can be below the threshold, so the filter must never run; A103: "If the RPC is
    // not sampled, then the filter will pass the RPC through to the next filter ... without
    // delegating to any nested filters."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void samplePercentZero_nestedFilterNeverRuns() {
      pushCompositeConfig(matcherOnHeader("run",
          executeActionSampled("fault", 0, faultAbort(Status.Code.PERMISSION_DENIED))));

      assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
    }

    @Test
    public void samplePercentHundred_nestedFilterAlwaysRuns() {
      pushCompositeConfig(matcherOnHeader("run",
          executeActionSampled("fault", 100, faultAbort(Status.Code.PERMISSION_DENIED))));

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // ---------------------------------------------------------------------------------------------
    // A103 on filter_chain: "This specifies a chain of filters to call, in order."
    //
    // Both filters in the chain abort, with different status codes. Whichever runs first terminates
    // the RPC, so the resulting status code names the filter that ran first. "In order" means that
    // must be the one listed first.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void filterChain_callsFiltersInConfiguredOrder() {
      pushCompositeConfig(matcherOnHeader("run", executeActionChain("chain",
          faultAbort(Status.Code.PERMISSION_DENIED),   // listed first -> must run first
          faultAbort(Status.Code.RESOURCE_EXHAUSTED))));

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // ---------------------------------------------------------------------------------------------
    // A103 on ExtensionWithMatcherPerRoute: "The value of this field will replace the value of the
    // field in the top-level config."
    //
    // Replacement, not merge: after the override is applied the top-level matcher must have no
    // effect at all, so an RPC that the top-level matcher would have matched now finds no match.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void perRouteOverride_replacesTopLevelMatcher() {
      // Top level matches MATCH_HEADER; the override matches OTHER_HEADER instead.
      Matcher topLevel = matcherOnHeader("run",
          executeAction("fault", faultAbort(Status.Code.PERMISSION_DENIED)));
      Matcher override = matcherOnOtherHeader("run",
          executeAction("fault", faultAbort(Status.Code.RESOURCE_EXHAUSTED)));

      pushCompositeConfig(topLevel);
      pushRouteOverride(override);

      // The override's matcher is in force, so the override's nested filter runs.
      StatusRuntimeException overridden = assertThrows(StatusRuntimeException.class,
          () -> callWithHeader(OTHER_HEADER, "run"));
      assertThat(overridden.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);

      // The top-level matcher was replaced, not merged, so its header no longer matches anything.
      StatusRuntimeException replaced = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());
      assertThat(replaced.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "if a nested filter chain includes a filter that is not supported on the side that
    // running on, we will fail the RPC with status UNAVAILABLE."
    //
    // RBAC is a server-side-only filter, nested here inside a client-side composite filter.
    // ---------------------------------------------------------------------------------------------

    @Test
    public void nestedFilterUnsupportedOnThisSide_rpcFailsWithUnavailable() {
      pushCompositeConfig(matcherOnHeader("run",
          executeAction("rbac", Any.pack(RBAC.getDefaultInstance()))));

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    // ---------------------------------------------------------------------------------------------
    // RPC helpers
    // ---------------------------------------------------------------------------------------------

    private SimpleResponse callWithMatchHeader() {
      return callWithHeader(MATCH_HEADER, "run");
    }

    private SimpleResponse callWithHeader(Metadata.Key<String> key, String value) {
      Metadata headers = new Metadata();
      headers.put(key, value);
      return stub().withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
          .unaryRpc(SimpleRequest.getDefaultInstance());
    }

    private SimpleResponse callWithoutHeaders() {
      return stub().unaryRpc(SimpleRequest.getDefaultInstance());
    }

    private SimpleServiceGrpc.SimpleServiceBlockingStub stub() {
      ManagedChannel channel = dataPlane.getManagedChannel();
      return SimpleServiceGrpc.newBlockingStub(channel)
          .withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------------------------------------
    // Config plumbing
    // ---------------------------------------------------------------------------------------------

    /** Replaces the client listener with one whose filter chain is [composite, router]. */
    private void pushCompositeConfig(Matcher matcher) {
      ExtensionWithMatcher compositeConfig = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder()
              .setName(COMPOSITE_FILTER_NAME)
              .setTypedConfig(Any.pack(Composite.getDefaultInstance())))
          .setXdsMatcher(matcher)
          .build();

      HttpFilter composite = HttpFilter.newBuilder()
          .setName(COMPOSITE_FILTER_NAME)
          .setTypedConfig(Any.pack(compositeConfig))
          .build();
      HttpFilter router = HttpFilter.newBuilder()
          .setName("terminal-filter")
          .setTypedConfig(Any.pack(Router.newBuilder().build()))
          .setIsOptional(true)
          .build();

      ApiListener apiListener = ApiListener.newBuilder()
          .setApiListener(Any.pack(HttpConnectionManager.newBuilder()
              .setRds(Rds.newBuilder()
                  .setRouteConfigName(RDS_NAME)
                  .setConfigSource(ConfigSource.newBuilder()
                      .setAds(AggregatedConfigSource.getDefaultInstance())))
              .addHttpFilters(composite)
              .addHttpFilters(router)
              .build(), HTTP_CONNECTION_MANAGER_TYPE_URL))
          .build();

      controlPlane.setLdsConfig(
          ControlPlaneRule.buildServerListener(),
          Listener.newBuilder().setName(SERVER_HOST_NAME).setApiListener(apiListener).build());
    }

    /** Attaches an ExtensionWithMatcherPerRoute override to the one and only route. */
    private void pushRouteOverride(Matcher matcher) {
      ExtensionWithMatcherPerRoute perRoute = ExtensionWithMatcherPerRoute.newBuilder()
          .setXdsMatcher(matcher)
          .build();

      controlPlane.setRdsConfig(RouteConfiguration.newBuilder()
          .setName(RDS_NAME)
          .addVirtualHosts(VirtualHost.newBuilder()
              .setName(RDS_NAME)
              .addDomains(SERVER_HOST_NAME)
              .addRoutes(Route.newBuilder()
                  .setMatch(RouteMatch.newBuilder().setPrefix("/"))
                  .setRoute(RouteAction.newBuilder()
                      .setCluster(CLUSTER_NAME)
                      .setAutoHostRewrite(BoolValue.of(true)))
                  .putTypedPerFilterConfig(COMPOSITE_FILTER_NAME, Any.pack(perRoute))))
          .build());
    }

    // ---------------------------------------------------------------------------------------------
    // Proto builders (plain xDS API usage, per A103 / A106)
    // ---------------------------------------------------------------------------------------------

    private static Matcher matcherOnHeader(String value, Matcher.OnMatch onMatch) {
      return matcherOnNamedHeader(MATCH_HEADER.name(), value, onMatch);
    }

    private static Matcher matcherOnOtherHeader(String value, Matcher.OnMatch onMatch) {
      return matcherOnNamedHeader(OTHER_HEADER.name(), value, onMatch);
    }

    /** A single-predicate matcher list, with no on_no_match, keyed on one request header. */
    private static Matcher matcherOnNamedHeader(
        String headerName, String headerValue, Matcher.OnMatch onMatch) {
      return Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                  .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
                      .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                          .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                              .setName("request-headers")
                              .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                                  .setHeaderName(headerName)
                                  .build())))
                          .setValueMatch(StringMatcher.newBuilder().setExact(headerValue))))
                  .setOnMatch(onMatch)))
          .build();
    }

    /** A103: a SkipFilter action "indicates that no filter will be executed". */
    private static Matcher.OnMatch skipAction() {
      return action("skip", Any.pack(SkipFilter.getDefaultInstance()));
    }

    /** An ExecuteFilterAction naming a single nested filter via typed_config. */
    private static Matcher.OnMatch executeAction(String name, Any childConfig) {
      return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName(name)
              .setTypedConfig(childConfig))
          .build()));
    }

    /** The same, but with sample_percent.default_value set to {@code percent}%. */
    private static Matcher.OnMatch executeActionSampled(String name, int percent, Any childConfig) {
      return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName(name)
              .setTypedConfig(childConfig))
          .setSamplePercent(io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent.newBuilder()
              .setDefaultValue(percentage(percent)))
          .build()));
    }

    /** An ExecuteFilterAction naming several nested filters via filter_chain, in order. */
    private static Matcher.OnMatch executeActionChain(String name, Any... childConfigs) {
      FilterChainConfiguration.Builder chain = FilterChainConfiguration.newBuilder();
      for (int i = 0; i < childConfigs.length; i++) {
        chain.addTypedConfig(TypedExtensionConfig.newBuilder()
            .setName(name + "-" + i)
            .setTypedConfig(childConfigs[i]));
      }
      return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
          .setFilterChain(chain)
          .build()));
    }

    private static Matcher.OnMatch action(String actionName, Any actionConfig) {
      return Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName(actionName)
              .setTypedConfig(actionConfig))
          .build();
    }

    /** A fault filter config that aborts 100% of RPCs with {@code code}. */
    private static Any faultAbort(Status.Code code) {
      return Any.pack(HTTPFault.newBuilder()
          .setAbort(FaultAbort.newBuilder()
              .setGrpcStatus(code.value())
              .setPercentage(percentage(100)))
          .build());
    }

    private static FractionalPercent percentage(int percent) {
      return FractionalPercent.newBuilder()
          .setNumerator(percent)
          .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
          .build();
    }
  }

  @RunWith(JUnit4.class)
  public static class ServerE2eTest {

    private static final String SERVER_HOST_NAME = "test-server";
    private static final String SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT =
        "grpc/server?udpa.resource.listening_address=";
    private static final String COMPOSITE_FILTER_NAME = "composite-filter";

    private static final Metadata.Key<String> MATCH_HEADER =
        Metadata.Key.of("x-composite-match", Metadata.ASCII_STRING_MARSHALLER);

    private static final SimpleResponse EXPECTED_RESPONSE = SimpleResponse.newBuilder()
        .setResponseMessage("Hi, xDS! Authority= " + SERVER_HOST_NAME)
        .build();

    /** How long to wait for a pushed server listener to reach the already-running xDS server. */
    private static final long CONFIG_PROPAGATION_TIMEOUT_SECONDS = 15;

    @Rule(order = 0)
    public ControlPlaneRule controlPlane = new ControlPlaneRule();

    @Rule(order = 1)
    public DataPlaneRule dataPlane = new DataPlaneRule(controlPlane);

    @BeforeClass
    public static void enableCompositeFilter() {
      System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    }

    @AfterClass
    public static void disableCompositeFilter() {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "If the matcher tree does not find a match, the RPC will be failed with UNAVAILABLE
    // status."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void noMatch_rpcFailsWithUnavailable() {
      pushServerCompositeConfig(executeRbacAction());
      awaitCompositeFilterActive();

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> stub().unaryRpc(SimpleRequest.getDefaultInstance()));

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "If the matcher tree finds a SkipFilter match, the filter will simply pass the RPC
    // through to the next filter ... without delegating to any nested filters."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void skipFilterAction_rpcPassesThrough() {
      pushServerCompositeConfig(skipAction());
      awaitCompositeFilterActive();

      assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "the RPC will be passed to the nested filter chain before being sent to the next
    // filter".
    // ---------------------------------------------------------------------------------------------

    @Test
    public void executeFilterAction_matched_nestedFilterRuns() {
      pushServerCompositeConfig(executeRbacAction());
      awaitCompositeFilterActive();

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // ---------------------------------------------------------------------------------------------
    // A103: "If the RPC is not sampled, then the filter will pass the RPC through to the next
    // ... without delegating to any nested filters."
    // ---------------------------------------------------------------------------------------------

    @Test
    public void samplePercentZero_nestedFilterNeverRuns() {
      pushServerCompositeConfig(executeRbacActionSampled(0));
      awaitCompositeFilterActive();

      assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
    }

    @Test
    public void samplePercentHundred_nestedFilterAlwaysRuns() {
      pushServerCompositeConfig(executeRbacActionSampled(100));
      awaitCompositeFilterActive();

      StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
          () -> callWithMatchHeader());

      assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    /**
     * Blocks until the pushed server listener is actually in force.
     *
     * <p>Unlike the client tests, the xDS server is already running and serving the default
     * by the time a test body starts, so an assertion made too early can pass against the *old*
     * config. Every matcher in this class keys off {@link #MATCH_HEADER} and declares no
     * {@code on_no_match}, so a header-less RPC returns UNAVAILABLE once - and only once - the
     * composite filter is live. That makes it a reliable barrier.
     */
    private void awaitCompositeFilterActive() {
      long deadline = System.nanoTime()
          + TimeUnit.SECONDS.toNanos(CONFIG_PROPAGATION_TIMEOUT_SECONDS);
      Status.Code lastCode = null;
      while (System.nanoTime() < deadline) {
        try {
          stub().unaryRpc(SimpleRequest.getDefaultInstance());
          lastCode = Status.Code.OK;
        } catch (StatusRuntimeException e) {
          lastCode = e.getStatus().getCode();
          if (lastCode == Status.Code.UNAVAILABLE) {
            return;
          }
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new AssertionError("interrupted waiting for config propagation", e);
        }
      }
      throw new AssertionError(
          "composite filter never became active; last RPC status was " + lastCode);
    }

    private SimpleResponse callWithMatchHeader() {
      Metadata headers = new Metadata();
      headers.put(MATCH_HEADER, "run");
      return stub().withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
          .unaryRpc(SimpleRequest.getDefaultInstance());
    }

    private SimpleServiceGrpc.SimpleServiceBlockingStub stub() {
      ManagedChannel channel = dataPlane.getManagedChannel();
      return SimpleServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    /**
     * Replaces the server listener with one whose HTTP filter chain is [composite, router]. The
     * client listener is left at its default, so only the server side is under test.
     */
    private void pushServerCompositeConfig(Matcher.OnMatch onMatch) {
      ExtensionWithMatcher compositeConfig = ExtensionWithMatcher.newBuilder()
          .setExtensionConfig(TypedExtensionConfig.newBuilder()
              .setName(COMPOSITE_FILTER_NAME)
              .setTypedConfig(Any.pack(Composite.getDefaultInstance())))
          .setXdsMatcher(matcherOnMatchHeader(onMatch))
          .build();

      HttpFilter composite = HttpFilter.newBuilder()
          .setName(COMPOSITE_FILTER_NAME)
          .setTypedConfig(Any.pack(compositeConfig))
          .build();
      HttpFilter router = HttpFilter.newBuilder()
          .setName("terminal-filter")
          .setTypedConfig(Any.pack(Router.newBuilder().build()))
          .setIsOptional(true)
          .build();

      RouteConfiguration routeConfig = RouteConfiguration.newBuilder()
          .addVirtualHosts(VirtualHost.newBuilder()
              .setName("virtual-host-0")
              .addDomains("*")
              .addRoutes(Route.newBuilder()
                  .setMatch(RouteMatch.newBuilder().setPrefix("/"))
                  .setNonForwardingAction(NonForwardingAction.getDefaultInstance())))
          .build();

      io.envoyproxy.envoy.config.listener.v3.Filter networkFilter =
          io.envoyproxy.envoy.config.listener.v3.Filter.newBuilder()
          .setName("network-filter-0")
          .setTypedConfig(Any.pack(HttpConnectionManager.newBuilder()
              .setRouteConfig(routeConfig)
              .addHttpFilters(composite)
              .addHttpFilters(router)
              .build()))
          .build();

      Listener serverListener = Listener.newBuilder()
          .setName(SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT)
          .setTrafficDirection(TrafficDirection.INBOUND)
          .addFilterChains(FilterChain.newBuilder()
              .setName("filter-chain-0")
              .setFilterChainMatch(FilterChainMatch.newBuilder()
                  .setSourceType(FilterChainMatch.ConnectionSourceType.ANY))
              .addFilters(networkFilter))
          .setAddress(Address.newBuilder()
              .setSocketAddress(SocketAddress.newBuilder().setAddress("0.0.0.0").setPortValue(0)))
          .build();

      controlPlane.setLdsConfig(
          serverListener, ControlPlaneRule.buildClientListener(SERVER_HOST_NAME));
    }

    // ---------------------------------------------------------------------------------------------
    // Proto builders
    // ---------------------------------------------------------------------------------------------

    private static Matcher matcherOnMatchHeader(Matcher.OnMatch onMatch) {
      return Matcher.newBuilder()
          .setMatcherList(Matcher.MatcherList.newBuilder()
              .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                  .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
                      .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                          .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                              .setName("request-headers")
                              .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                                  .setHeaderName(MATCH_HEADER.name())
                                  .build())))
                          .setValueMatch(StringMatcher.newBuilder().setExact("run"))))
                  .setOnMatch(onMatch)))
          .build();
    }

    private static Matcher.OnMatch skipAction() {
      return action("skip", Any.pack(SkipFilter.getDefaultInstance()));
    }

    private static Matcher.OnMatch executeRbacAction() {
      return action("execute-rbac", Any.pack(ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("rbac")
              .setTypedConfig(denyAllRbac()))
          .build()));
    }

    private static Matcher.OnMatch executeRbacActionSampled(int percent) {
      return action("execute-rbac", Any.pack(ExecuteFilterAction.newBuilder()
          .setTypedConfig(TypedExtensionConfig.newBuilder()
              .setName("rbac")
              .setTypedConfig(denyAllRbac()))
          .setSamplePercent(io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent.newBuilder()
              .setDefaultValue(FractionalPercent.newBuilder()
                  .setNumerator(percent)
                  .setDenominator(FractionalPercent.DenominatorType.HUNDRED)))
          .build()));
    }

    private static Matcher.OnMatch action(String actionName, Any actionConfig) {
      return Matcher.OnMatch.newBuilder()
          .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
              .setName(actionName)
              .setTypedConfig(actionConfig))
          .build();
    }

    /** An ALLOW policy set with no policies in it: nothing can match, so everything is denied. */
    private static Any denyAllRbac() {
      return Any.pack(RBAC.newBuilder()
          .setRules(io.envoyproxy.envoy.config.rbac.v3.RBAC.newBuilder()
              .setAction(io.envoyproxy.envoy.config.rbac.v3.RBAC.Action.ALLOW))
          .build());
    }
  }
}
