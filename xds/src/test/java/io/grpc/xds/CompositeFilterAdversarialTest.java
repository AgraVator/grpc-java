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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.github.udpa.udpa.type.v1.TypedStruct;
import com.github.xds.type.matcher.v3.Matcher;
import com.github.xds.type.matcher.v3.StringMatcher;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
import io.envoyproxy.envoy.type.matcher.v3.HttpRequestHeaderMatchInput;
import io.grpc.xds.Filter.FilterConfig;
import io.grpc.xds.Filter.FilterConfigParseContext;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Adversarial, boundary, and spec compliance tests for {@link CompositeFilter}
 * per gRFC A103.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompositeFilterAdversarialTest {

  private static final String FAKE_TYPE_URL = "type.googleapis.com/fake";
  private static final String FAKE_FAILING_PARSE_TYPE_URL = "type.googleapis.com/fake.failing";
  private static final String EXECUTE_ACTION_TYPE_URL =
      "type.googleapis.com/envoy.extensions.filters.http.composite.v3.ExecuteFilterAction";

  private CompositeFilter.Provider provider;

  @Mock
  private Filter.Provider fakeProvider;
  @Mock
  private Filter.Provider fakeFailingProvider;
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
    ConfigOrError<? extends FilterConfig> configRes = ConfigOrError.fromConfig(fakeConfig);
    when(fakeProvider.parseFilterConfig(any(com.google.protobuf.Message.class), any()))
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

  private static ExtensionWithMatcher createExtensionWithMatcher(Matcher matcher) {
    return ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .setXdsMatcher(matcher)
        .build();
  }

  private static Matcher.OnMatch createExecuteAction(String childName, String typeUrl) {
    return Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_" + childName)
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl(EXECUTE_ACTION_TYPE_URL)
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
                .putMap("outerKey", Matcher.OnMatch.newBuilder().setMatcher(innerMatcher).build())))
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
        provider.parseFilterConfig(Any.pack(createExtensionWithMatcher(root)), getFilterContext());

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
  public void actionTypeUrl_bareFilterDirectlyAsOnMatchAction_failsOpenOrRejected() {
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
    // ExecuteFilterAction"
    if (result.errorDetail == null) {
      // Check if it was added to delegates map or if it was dropped (failing open!)
      boolean addedToDelegates = result.config.delegates.containsKey(bareAction);
      if (!addedToDelegates) {
        fail("FAIL-OPEN VULNERABILITY: Bare filter provider was accepted as action, but omitted "
            + "from delegates (fails open at runtime)!");
      }
    } else {
      assertThat(result.errorDetail).contains("Expected ExecuteFilterAction or SkipFilter but got");
    }
  }

  @Test
  public void actionTypeUrl_executeFilterAction_emptyConfig_failsOpenOrRejected() {
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
    if (result.errorDetail == null) {
      fail("SPEC VIOLATION / FAIL-OPEN: ExecuteFilterAction with neither typed_config nor "
          + "filter_chain was accepted without error!");
    }
  }

  @Test
  public void actionTypeUrl_executeFilterAction_emptyFilterChain_failsOpenOrRejected() {
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

    if (result.errorDetail == null) {
      fail("SPEC VIOLATION / FAIL-OPEN: ExecuteFilterAction with empty filter_chain was "
          + "accepted without error!");
    }
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
        "Nested filter cannot be a terminal filter (RouterFilter)");
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
        "Nested filter cannot be a terminal filter (RouterFilter)");
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
        "Nested filter cannot be a terminal filter (RouterFilter)");
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
        "Nested filter cannot be a terminal filter (RouterFilter)");
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
        "Nested filter cannot be a terminal filter (RouterFilter)");
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
        "Invalid message type: io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite");
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
  public void envelope_missingXdsMatcher_succeedsAsNoOp() {
    ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());
    assertThat(result.errorDetail).isNull();
    assertThat(result.config).isNotNull();
    assertThat(result.config.matcher).isNull();
  }

  @Test
  public void envelopeOverride_nonAnyRawMessage_rejected() {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(Composite.getDefaultInstance(), getFilterContext());
    assertThat(result.errorDetail).contains(
        "Invalid message type: io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite");
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
  public void envelopeOverride_missingXdsMatcher_rejected() {
    // An override with no matcher configures nothing, so it is rejected rather than silently
    // ignored.
    ExtensionWithMatcherPerRoute proto = ExtensionWithMatcherPerRoute.newBuilder().build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(Any.pack(proto), getFilterContext());
    assertThat(result.config).isNull();
    assertThat(result.errorDetail)
        .contains("ExtensionWithMatcherPerRoute.xds_matcher: field not set");
  }
}
