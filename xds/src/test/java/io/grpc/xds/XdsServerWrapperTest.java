/*
 * Copyright 2021 The gRPC Authors
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
import static com.google.common.truth.Truth.assertWithMessage;
import static io.grpc.xds.XdsServerWrapper.ATTR_SERVER_ROUTING_CONFIG;
import static io.grpc.xds.XdsServerWrapper.RETRY_DELAY_NANOS;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.core.v3.SocketAddress.Protocol;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
import io.envoyproxy.envoy.type.matcher.v3.HttpRequestHeaderMatchInput;
import io.grpc.Attributes;
import io.grpc.ChannelConfigurator;
import io.grpc.InsecureChannelCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import io.grpc.internal.FakeClock;
import io.grpc.internal.ObjectPool;
import io.grpc.testing.TestMethodDescriptors;
import io.grpc.xds.EnvoyServerProtoData.CidrRange;
import io.grpc.xds.EnvoyServerProtoData.FilterChain;
import io.grpc.xds.EnvoyServerProtoData.FilterChainMatch;
import io.grpc.xds.EnvoyServerProtoData.Listener;
import io.grpc.xds.Filter.FilterConfig;
import io.grpc.xds.Filter.FilterContext;
import io.grpc.xds.Filter.NamedFilterConfig;
import io.grpc.xds.FilterChainMatchingProtocolNegotiators.FilterChainMatchingHandler.FilterChainSelector;
import io.grpc.xds.StatefulFilter.Config;
import io.grpc.xds.VirtualHost.Route;
import io.grpc.xds.VirtualHost.Route.RouteMatch;
import io.grpc.xds.VirtualHost.Route.RouteMatch.PathMatcher;
import io.grpc.xds.XdsListenerResource.LdsUpdate;
import io.grpc.xds.XdsRouteConfigureResource.RdsUpdate;
import io.grpc.xds.XdsServerBuilder.XdsServingStatusListener;
import io.grpc.xds.XdsServerTestHelper.FakeXdsClient;
import io.grpc.xds.XdsServerTestHelper.FakeXdsClientPoolFactory;
import io.grpc.xds.XdsServerWrapper.ConfigApplyingInterceptor;
import io.grpc.xds.XdsServerWrapper.ServerRoutingConfig;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData;
import io.grpc.xds.client.XdsClient;
import io.grpc.xds.client.XdsClient.ResourceWatcher;
import io.grpc.xds.internal.Matchers.HeaderMatcher;
import io.grpc.xds.internal.security.CommonTlsContextTestsUtil;
import io.grpc.xds.internal.security.SslContextProviderSupplier;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class XdsServerWrapperTest {
  private static final int START_WAIT_AFTER_LISTENER_MILLIS = 100;
  private static final String ROUTER_FILTER_INSTANCE_NAME = "envoy.router";
  private static final RouterFilter.Provider ROUTER_FILTER_PROVIDER = new RouterFilter.Provider();

  // Readability: makes it simpler to distinguish resource parameters.
  private static final ImmutableMap<String, FilterConfig> NO_FILTER_OVERRIDES = ImmutableMap.of();

  private static final String STATEFUL_1 = "stateful_1";
  private static final String STATEFUL_2 = "stateful_2";

  @Rule
  public final MockitoRule mocks = MockitoJUnit.rule();

  @Mock
  private ServerBuilder<?> mockBuilder;
  @Mock
  private Server mockServer;
  @Mock
  private XdsServingStatusListener listener;

  private FilterChainSelectorManager selectorManager = new FilterChainSelectorManager();
  private FakeClock executor = new FakeClock();
  private FakeXdsClient xdsClient = new FakeXdsClient();
  private FilterRegistry filterRegistry = FilterRegistry.getDefaultRegistry();
  private XdsServerWrapper xdsServerWrapper;
  private CompositeFilter.Provider compositeProvider;
  private ServerRoutingConfig noopConfig = ServerRoutingConfig.create(
      ImmutableList.<VirtualHost>of(), ImmutableMap.<Route, ServerInterceptor>of());
  // XdsServerWrapper's syncContext swallows exceptions thrown by an update and logs them at
  // SEVERE, so tests that must not tolerate one watch the log.
  private final List<LogRecord> severeLogs = new CopyOnWriteArrayList<>();
  private final Handler severeLogHandler = new Handler() {
    @Override
    public void publish(LogRecord record) {
      if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
        severeLogs.add(record);
      }
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  };

  @Before
  public void setup() {
    Logger.getLogger(XdsServerWrapper.class.getName()).addHandler(severeLogHandler);
    when(mockBuilder.build()).thenReturn(mockServer);
    xdsServerWrapper = new XdsServerWrapper("0.0.0.0:1", mockBuilder, listener,
            selectorManager, new FakeXdsClientPoolFactory(xdsClient),
            XdsServerTestHelper.RAW_BOOTSTRAP,
            filterRegistry, executor.getScheduledExecutorService());
  }

  @After
  public void tearDown() {
    Logger.getLogger(XdsServerWrapper.class.getName()).removeHandler(severeLogHandler);
    xdsServerWrapper.shutdownNow();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBootstrap() throws Exception {
    Bootstrapper.BootstrapInfo b =
        Bootstrapper.BootstrapInfo.builder()
            .servers(Arrays.asList(
                Bootstrapper.ServerInfo.create("uri", InsecureChannelCredentials.create())))
            .node(EnvoyProtoData.Node.newBuilder().setId("id").build())
            .serverListenerResourceNameTemplate("grpc/server?udpa.resource.listening_address=%s")
            .build();
    XdsClient xdsClient = mock(XdsClient.class);
    XdsListenerResource listenerResource = XdsListenerResource.getInstance();
    when(xdsClient.getBootstrapInfo()).thenReturn(b);
    xdsServerWrapper = new XdsServerWrapper("[::FFFF:129.144.52.38]:80", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP, filterRegistry);
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          xdsServerWrapper.start();
        } catch (IOException ex) {
          // ignore
        }
      }
    });
    verify(xdsClient, timeout(5000)).watchXdsResource(
        eq(listenerResource),
        eq("grpc/server?udpa.resource.listening_address=[::FFFF:129.144.52.38]:80"),
        any(ResourceWatcher.class),
        any(SynchronizationContext.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBootstrap_ldsResourceNameResolver() throws Exception {
    Bootstrapper.BootstrapInfo b =
        Bootstrapper.BootstrapInfo.builder()
            .servers(
                Arrays.asList(
                    Bootstrapper.ServerInfo.create("uri", InsecureChannelCredentials.create())))
            .node(EnvoyProtoData.Node.newBuilder().setId("id").build())
            .serverListenerResourceNameTemplate("grpc/server?udpa.resource.listening_address=%s")
            .build();
    XdsClient xdsClient = mock(XdsClient.class);
    XdsListenerResource listenerResource = XdsListenerResource.getInstance();
    when(xdsClient.getBootstrapInfo()).thenReturn(b);
    xdsServerWrapper =
        new XdsServerWrapper(
            "[::FFFF:129.144.52.38]:80",
            mockBuilder,
            listener,
            selectorManager,
            new FakeXdsClientPoolFactory(xdsClient),
            XdsServerTestHelper.RAW_BOOTSTRAP,
            addr -> "xdstp://resolved_name/" + addr,
            filterRegistry);
    Executors.newSingleThreadExecutor()
        .execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  xdsServerWrapper.start();
                } catch (IOException ex) {
                  // ignore
                }
              }
            });
    verify(xdsClient, timeout(5000))
        .watchXdsResource(
            eq(listenerResource),
            eq("xdstp://resolved_name/[::FFFF:129.144.52.38]:80"),
            any(ResourceWatcher.class),
            any(SynchronizationContext.class));
  }

  @Test
  public void testBootstrap_noTemplate() throws Exception {
    Bootstrapper.BootstrapInfo b =
        Bootstrapper.BootstrapInfo.builder()
            .servers(Arrays.asList(
                Bootstrapper.ServerInfo.create("uri", InsecureChannelCredentials.create())))
            .node(EnvoyProtoData.Node.newBuilder().setId("id").build())
            .build();
    verifyBootstrapFail(b);
  }

  private void verifyBootstrapFail(Bootstrapper.BootstrapInfo b) throws Exception {
    XdsClient xdsClient = mock(XdsClient.class);
    when(xdsClient.getBootstrapInfo()).thenReturn(b);
    xdsServerWrapper = new XdsServerWrapper("0.0.0.0:1", mockBuilder, listener,
            selectorManager, new FakeXdsClientPoolFactory(xdsClient),
            XdsServerTestHelper.RAW_BOOTSTRAP, filterRegistry);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    try {
      start.get(5000, TimeUnit.MILLISECONDS);
      fail("Start should throw exception");
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IOException.class);
      Throwable cause = ex.getCause().getCause();
      assertThat(cause).isInstanceOf(StatusException.class);
      assertThat(((StatusException)cause).getStatus().getCode())
              .isEqualTo(Status.UNAVAILABLE.getCode());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBootstrap_templateWithXdstp() throws Exception {
    Bootstrapper.BootstrapInfo b = Bootstrapper.BootstrapInfo.builder()
        .servers(Arrays.asList(
            Bootstrapper.ServerInfo.create(
                "uri", InsecureChannelCredentials.create())))
        .node(EnvoyProtoData.Node.newBuilder().setId("id").build())
        .serverListenerResourceNameTemplate(
            "xdstp://xds.authority.com/envoy.config.listener.v3.Listener/grpc/server/%s")
        .build();
    XdsClient xdsClient = mock(XdsClient.class);
    XdsListenerResource listenerResource = XdsListenerResource.getInstance();
    when(xdsClient.getBootstrapInfo()).thenReturn(b);
    xdsServerWrapper = new XdsServerWrapper("[::FFFF:129.144.52.38]:80", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP, filterRegistry);
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          xdsServerWrapper.start();
        } catch (IOException ex) {
          // ignore
        }
      }
    });
    verify(xdsClient, timeout(5000)).watchXdsResource(
        eq(listenerResource),
        eq("xdstp://xds.authority.com/envoy.config.listener.v3.Listener/grpc/server/"
            + "%5B::FFFF:129.144.52.38%5D:80"),
        any(ResourceWatcher.class),
        any(SynchronizationContext.class));
  }

  @Test
  public void shutdown() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    HttpConnectionManager hcm_virtual = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(createVirtualHost("virtual-host-0")),
            new ArrayList<NamedFilterConfig>());
    FilterChain f0 = createFilterChain("filter-chain-0", hcm_virtual);
    FilterChain f1 = createFilterChain("filter-chain-1", createRds("rds"));
    xdsClient.deliverLdsUpdate(Collections.singletonList(f0), f1);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    verify(listener, timeout(5000)).onServing();
    start.get(START_WAIT_AFTER_LISTENER_MILLIS, TimeUnit.MILLISECONDS);
    verify(mockServer).start();
    xdsServerWrapper.shutdown();
    assertThat(xdsServerWrapper.isShutdown()).isTrue();
    assertThat(xdsClient.ldsResource).isNull();
    assertThat(xdsClient.isShutDown()).isTrue();
    verify(mockServer).shutdown();
    assertThat(f0.sslContextProviderSupplier().isShutdown()).isTrue();
    assertThat(f1.sslContextProviderSupplier().isShutdown()).isTrue();
    when(mockServer.isTerminated()).thenReturn(true);
    when(mockServer.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
    assertThat(xdsServerWrapper.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    xdsServerWrapper.awaitTermination();
    assertThat(xdsServerWrapper.isTerminated()).isTrue();
    assertThat(start.get()).isSameInstanceAs(xdsServerWrapper);
  }

  @Test
  public void shutdown_inflight() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    HttpConnectionManager hcmVirtual = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(createVirtualHost("virtual-host-0")),
            new ArrayList<NamedFilterConfig>());
    FilterChain f0 = createFilterChain("filter-chain-0", createRds("rds"));
    FilterChain f1 = createFilterChain("filter-chain-1", hcmVirtual);
    xdsClient.deliverLdsUpdate(Collections.singletonList(f0), f1);
    xdsServerWrapper.shutdown();
    when(mockServer.isTerminated()).thenReturn(true);
    when(mockServer.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
    assertThat(xdsServerWrapper.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    xdsServerWrapper.awaitTermination();
    assertThat(xdsServerWrapper.isTerminated()).isTrue();
    verify(mockServer, never()).start();
    assertThat(xdsServerWrapper.isShutdown()).isTrue();
    assertThat(xdsClient.ldsResource).isNull();
    assertThat(xdsClient.isShutDown()).isTrue();
    verify(mockServer).shutdown();
    assertThat(f0.sslContextProviderSupplier().isShutdown()).isTrue();
    assertThat(f1.sslContextProviderSupplier().isShutdown()).isTrue();
    assertThat(start.isDone()).isFalse(); //shall we set initialStatus when shutdown?
  }

  @Test
  public void shutdown_afterResourceNotExist() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    Status notFoundStatus = Status.NOT_FOUND.withDescription("Resource not found: " + ldsResource);
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    verify(listener, timeout(5000)).onNotServing(any());
    try {
      start.get(START_WAIT_AFTER_LISTENER_MILLIS, TimeUnit.MILLISECONDS);
      fail("server should not start() successfully.");
    } catch (TimeoutException ex) {
      // expect to block here.
      assertThat(start.isDone()).isFalse();
    }
    verify(mockBuilder, times(1)).build();
    verify(mockServer, never()).start();
    verify(mockServer).shutdown();
    when(mockServer.isShutdown()).thenReturn(true);
    when(mockServer.isTerminated()).thenReturn(true);
    verify(listener, times(1)).onNotServing(any(Throwable.class));
    xdsServerWrapper.shutdown();
    assertThat(xdsServerWrapper.isShutdown()).isTrue();
    assertThat(xdsClient.ldsResource).isNull();
    assertThat(xdsClient.isShutDown()).isTrue();
    verify(mockBuilder, times(1)).build();
    verify(mockServer, times(1)).shutdown();
    xdsServerWrapper.awaitTermination(1, TimeUnit.SECONDS);
    assertThat(xdsServerWrapper.isTerminated()).isTrue();
  }

  @Test
  public void shutdown_pendingRetry() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    when(mockServer.start()).thenThrow(new IOException("error!"));
    FilterChain filterChain = createFilterChain("filter-chain-1", createRds("rds"));
    SslContextProviderSupplier sslSupplier = filterChain.sslContextProviderSupplier();
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    try {
      start.get(5000, TimeUnit.MILLISECONDS);
      fail("Start should throw exception");
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IOException.class);
    }
    assertThat(executor.getPendingTasks().size()).isEqualTo(1);
    verify(mockServer).start();
    verify(mockServer, never()).shutdown();
    xdsServerWrapper.shutdown();
    verify(mockServer).shutdown();
    when(mockServer.isTerminated()).thenReturn(true);
    assertThat(sslSupplier.isShutdown()).isTrue();
    assertThat(executor.getPendingTasks().size()).isEqualTo(0);
    verify(listener, never()).onNotServing(any(Throwable.class));
    verify(listener, never()).onServing();
    xdsServerWrapper.awaitTermination();
    assertThat(xdsServerWrapper.isTerminated()).isTrue();
  }

  @Test
  public void shutdownNow_startThreadShouldNotLeak() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor()
        .execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  start.set(xdsServerWrapper.start());
                } catch (Exception ex) {
                  start.setException(ex);
                }
              }
            });
    assertThat(xdsClient.ldsResource.get(5, TimeUnit.SECONDS))
        .isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    xdsServerWrapper.shutdownNow();
    try {
      start.get(5, TimeUnit.SECONDS);
      fail("should have thrown but not");
    } catch (ExecutionException ex) {
      assertThat(ex).hasCauseThat().isInstanceOf(IOException.class);
      assertThat(ex).hasCauseThat().hasMessageThat().isEqualTo("server is forcefully shut down");
    }
  }

  @Test
  public void shutdownNow_afterShutdown_stillUnblocksStartThread() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor()
        .execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  start.set(xdsServerWrapper.start());
                } catch (Exception ex) {
                  start.setException(ex);
                }
              }
            });
    assertThat(xdsClient.ldsResource.get(5, TimeUnit.SECONDS))
        .isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    xdsServerWrapper.shutdown();
    xdsServerWrapper.shutdownNow();
    try {
      start.get(5, TimeUnit.SECONDS);
      fail("should have thrown but not");
    } catch (ExecutionException ex) {
      assertThat(ex).hasCauseThat().isInstanceOf(IOException.class);
      assertThat(ex).hasCauseThat().hasMessageThat().isEqualTo("server is forcefully shut down");
    }
  }

  @Test
  public void shutdownNow_calledTwice_forcefullyShutsDownDelegateOnce() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor()
        .execute(
            new Runnable() {
              @Override
              public void run() {
                try {
                  start.set(xdsServerWrapper.start());
                } catch (Exception ex) {
                  start.setException(ex);
                }
              }
            });
    assertThat(xdsClient.ldsResource.get(5, TimeUnit.SECONDS))
        .isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    xdsServerWrapper.shutdownNow();
    xdsServerWrapper.shutdownNow();
    try {
      start.get(5, TimeUnit.SECONDS);
      fail("should have thrown but not");
    } catch (ExecutionException ex) {
      assertThat(ex).hasCauseThat().isInstanceOf(IOException.class);
    }
    verify(mockServer, times(1)).shutdownNow();
  }

  @Test
  public void initialStartIoException() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    when(mockServer.start()).thenThrow(new IOException("error!"));
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    FilterChain filterChain = createFilterChain("filter-chain-1", createRds("rds"));
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    try {
      start.get(5000, TimeUnit.MILLISECONDS);
      fail("Start should throw exception");
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IOException.class);
      assertThat(ex.getCause().getMessage()).isEqualTo("error!");
    }
  }

  @Test
  public void discoverState_virtualhost() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    VirtualHost virtualHost =
            VirtualHost.create(
                    "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
                    ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
            "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
            mock(TlsContextManager.class));
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    start.get(5000, TimeUnit.MILLISECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    ServerRoutingConfig realConfig =
        selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(filterChain).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(httpConnectionManager.virtualHosts());
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());
    verify(listener).onServing();
    verify(mockServer).start();
  }

  @Test
  public void discoverState_restart_afterResourceNotExist() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    VirtualHost virtualHost =
            VirtualHost.create(
                    "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
                    ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
            "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
            mock(TlsContextManager.class));
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    start.get(5000, TimeUnit.MILLISECONDS);
    verify(listener).onServing();
    verify(mockServer).start();

    // server shutdown after resourceDoesNotExist
    Status notFoundStatus = Status.NOT_FOUND.withDescription("Resource not found: " + ldsResource);
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    verify(mockServer).shutdown();

    // re-deliver lds resource
    reset(mockServer);
    reset(listener);
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    verify(listener).onServing();
    verify(mockServer).start();
  }

  @Test
  public void onChanged_listenerIsNull()
      throws ExecutionException, InterruptedException, TimeoutException {
    xdsServerWrapper = new XdsServerWrapper("10.1.2.3:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP,
        filterRegistry, executor.getScheduledExecutorService());
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=10.1.2.3:1");
    VirtualHost virtualHost =
        VirtualHost.create(
            "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());

    xdsClient.deliverLdsUpdateWithApiListener(0L, Arrays.asList(virtualHost));

    verify(listener, timeout(10000)).onNotServing(any());
  }

  @Test
  public void onChanged_listenerAddressMissingPort()
      throws ExecutionException, InterruptedException, TimeoutException {
    xdsServerWrapper = new XdsServerWrapper("10.1.2.3:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP,
        filterRegistry, executor.getScheduledExecutorService());
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=10.1.2.3:1");
    VirtualHost virtualHost =
        VirtualHost.create(
            "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
        0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
        "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
        mock(TlsContextManager.class));
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(
        Listener.create("listener", "20.3.4.5:",
            ImmutableList.copyOf(Collections.singletonList(filterChain)), null, Protocol.TCP));

    xdsClient.deliverLdsUpdate(listenerUpdate);

    verify(listener, timeout(10000)).onNotServing(any());
  }

  @Test
  public void onChanged_listenerAddressMismatch()
      throws ExecutionException, InterruptedException, TimeoutException {
    xdsServerWrapper = new XdsServerWrapper("10.1.2.3:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP,
        filterRegistry, executor.getScheduledExecutorService());
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=10.1.2.3:1");
    VirtualHost virtualHost =
        VirtualHost.create(
            "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
        0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
        "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
        mock(TlsContextManager.class));
    // A wildcard port must not make a mismatched listener address valid.
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(
        Listener.create("listener", "20.3.4.5:0",
            ImmutableList.copyOf(Collections.singletonList(filterChain)), null, Protocol.TCP));

    xdsClient.deliverLdsUpdate(listenerUpdate);

    verify(listener, timeout(10000)).onNotServing(any());
  }

  @Test
  public void onChanged_listenerAddressPortMismatch()
      throws ExecutionException, InterruptedException, TimeoutException {
    xdsServerWrapper = new XdsServerWrapper("10.1.2.3:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP,
        filterRegistry, executor.getScheduledExecutorService());
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=10.1.2.3:1");
    VirtualHost virtualHost =
        VirtualHost.create(
            "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
        0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
        "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
        mock(TlsContextManager.class));
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(
        Listener.create("listener", "10.1.2.3:2",
            ImmutableList.copyOf(Collections.singletonList(filterChain)), null, Protocol.TCP));

    xdsClient.deliverLdsUpdate(listenerUpdate);

    verify(listener, timeout(10000)).onNotServing(any());
  }

  @Test
  public void onChanged_listenerAddressWildcardPort()
      throws ExecutionException, InterruptedException, TimeoutException {
    xdsServerWrapper = new XdsServerWrapper("10.1.2.3:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP,
        filterRegistry, executor.getScheduledExecutorService());
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsResource).isEqualTo("grpc/server?udpa.resource.listening_address=10.1.2.3:1");
    VirtualHost virtualHost =
        VirtualHost.create(
            "virtual-host", Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());
    HttpConnectionManager httpConnectionManager = HttpConnectionManager.forVirtualHosts(
        0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain filterChain = EnvoyServerProtoData.FilterChain.create(
        "filter-chain-foo", createMatch(), httpConnectionManager, createTls(),
        mock(TlsContextManager.class));
    LdsUpdate listenerUpdate = LdsUpdate.forTcpListener(
        Listener.create("listener", "10.1.2.3:0",
            ImmutableList.copyOf(Collections.singletonList(filterChain)), null, Protocol.TCP));

    xdsClient.deliverLdsUpdate(listenerUpdate);

    verify(listener, timeout(10000)).onServing();
  }

  @Test
  public void discoverState_rds() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    VirtualHost virtualHost = createVirtualHost("virtual-host-0");
    HttpConnectionManager hcmVirtual = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain f0 = createFilterChain("filter-chain-0", hcmVirtual);
    EnvoyServerProtoData.FilterChain f1 = createFilterChain("filter-chain-1", createRds("r0"));
    xdsClient.setExpectedRdsCount(3);
    xdsClient.deliverLdsUpdate(Arrays.asList(f0, f1), null);
    assertThat(start.isDone()).isFalse();
    assertThat(selectorManager.getSelectorToUpdateSelector()).isNull();
    verify(mockServer, never()).start();
    verify(listener, never()).onServing();

    EnvoyServerProtoData.FilterChain f2 = createFilterChain("filter-chain-2", createRds("r1"));
    EnvoyServerProtoData.FilterChain f3 = createFilterChain("filter-chain-3", createRds("r2"));
    xdsClient.deliverLdsUpdate(Arrays.asList(f0, f2), f3);
    verify(mockServer, never()).start();
    verify(listener, never()).onServing();
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);

    xdsClient.deliverRdsUpdate("r1",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    verify(mockServer, never()).start();
    xdsClient.deliverRdsUpdate("r2",
            Collections.singletonList(createVirtualHost("virtual-host-2")));
    start.get(5000, TimeUnit.MILLISECONDS);
    verify(mockServer).start();
    ServerRoutingConfig realConfig =
        selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f0).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-0")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(2);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f2).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());
    realConfig = selectorManager.getSelectorToUpdateSelector().getDefaultRoutingConfig().get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-2")));
    assertThat(selectorManager.getSelectorToUpdateSelector().getDefaultSslContextProviderSupplier())
        .isEqualTo(f3.sslContextProviderSupplier());
  }

  @Test
  public void discoverState_oneRdsToMultipleFilterChain() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    EnvoyServerProtoData.FilterChain f0 = createFilterChain("filter-chain-0", createRds("r0"));
    EnvoyServerProtoData.FilterChain f1 = createFilterChain("filter-chain-1", createRds("r0"));
    EnvoyServerProtoData.FilterChain f2 = createFilterChain("filter-chain-2", createRds("r0"));

    xdsClient.deliverLdsUpdate(Arrays.asList(f0, f1), f2);
    assertThat(start.isDone()).isFalse();
    assertThat(selectorManager.getSelectorToUpdateSelector()).isNull();

    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("r0",
            Collections.singletonList(createVirtualHost("virtual-host-0")));
    start.get(5000, TimeUnit.MILLISECONDS);
    verify(mockServer, times(1)).start();
    ServerRoutingConfig realConfig =
        selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f0).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-0")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f1).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-0")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    realConfig = selectorManager.getSelectorToUpdateSelector().getDefaultRoutingConfig().get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-0")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());
    assertThat(selectorManager.getSelectorToUpdateSelector().getDefaultSslContextProviderSupplier())
        .isSameInstanceAs(f2.sslContextProviderSupplier());

    EnvoyServerProtoData.FilterChain f3 = createFilterChain("filter-chain-3", createRds("r0"));
    EnvoyServerProtoData.FilterChain f4 = createFilterChain("filter-chain-4", createRds("r1"));
    EnvoyServerProtoData.FilterChain f5 = createFilterChain("filter-chain-4", createRds("r1"));
    xdsClient.setExpectedRdsCount(1);
    xdsClient.deliverLdsUpdate(Arrays.asList(f5, f3), f4);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("r1",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    xdsClient.deliverRdsUpdate("r0",
            Collections.singletonList(createVirtualHost("virtual-host-0")));

    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(2);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f5).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f3).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-0")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    realConfig = selectorManager.getSelectorToUpdateSelector().getDefaultRoutingConfig().get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    assertThat(selectorManager.getSelectorToUpdateSelector().getDefaultSslContextProviderSupplier())
        .isSameInstanceAs(f4.sslContextProviderSupplier());
    verify(mockServer, times(1)).start();
    xdsServerWrapper.shutdown();
    verify(mockServer, times(1)).shutdown();
    when(mockServer.isTerminated()).thenReturn(true);
    xdsServerWrapper.awaitTermination();
    assertThat(xdsServerWrapper.isTerminated()).isTrue();
  }

  @Test
  public void discoverState_rds_onError_and_resourceNotExist() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsWatched = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    assertThat(ldsWatched).isEqualTo("grpc/server?udpa.resource.listening_address=0.0.0.0:1");
    VirtualHost virtualHost = createVirtualHost("virtual-host-0");
    HttpConnectionManager hcmVirtual = HttpConnectionManager.forVirtualHosts(
            0L, Collections.singletonList(virtualHost), new ArrayList<NamedFilterConfig>());
    EnvoyServerProtoData.FilterChain f0 = createFilterChain("filter-chain-0", hcmVirtual);
    EnvoyServerProtoData.FilterChain f1 = createFilterChain("filter-chain-1", createRds("r0"));
    xdsClient.deliverLdsUpdate(Arrays.asList(f0, f1), null);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.rdsWatchers.get("r0").onResourceChanged(StatusOr.fromStatus(Status.CANCELLED));
    start.get(5000, TimeUnit.MILLISECONDS);
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(2);
    ServerRoutingConfig realConfig =
        selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f1).get();
    assertThat(realConfig.virtualHosts()).isEmpty();
    assertThat(realConfig.interceptors()).isEmpty();

    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f0).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(hcmVirtual.virtualHosts());
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    xdsClient.deliverRdsUpdate("r0",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f1).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    xdsClient.rdsWatchers.get("r0").onAmbientError(Status.CANCELLED);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f1).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    Status notFoundStatus = Status.NOT_FOUND.withDescription("Resource r0 does not exist");
    xdsClient.rdsWatchers.get("r0").onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(f1).get();
    assertThat(realConfig.virtualHosts()).isEmpty();
    assertThat(realConfig.interceptors()).isEmpty();
  }

  @Test
  public void error() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    String ldsResource = xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    Status notFoundStatus = Status.NOT_FOUND.withDescription(
        "FakeXdsClient: Resource not found: " + ldsResource);
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    verify(listener, timeout(5000)).onNotServing(any());
    try {
      start.get(START_WAIT_AFTER_LISTENER_MILLIS, TimeUnit.MILLISECONDS);
      fail("server should not start()");
    } catch (TimeoutException ex) {
      // expect to block here.
      assertThat(start.isDone()).isFalse();
    }
    verify(listener, times(1)).onNotServing(any(StatusException.class));
    verify(mockBuilder, times(1)).build();
    FilterChain filterChain0 = createFilterChain("filter-chain-0", createRds("rds"));
    SslContextProviderSupplier sslSupplier0 = filterChain0.sslContextProviderSupplier();
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain0), null);
    ResourceWatcher<RdsUpdate> saveRdsWatcher = xdsClient.rdsWatchers.get("rds");
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(Status.INTERNAL));
    assertThat(selectorManager.getSelectorToUpdateSelector())
        .isSameInstanceAs(FilterChainSelector.NO_FILTER_CHAIN);
    verify(mockBuilder, times(1)).build();
    verify(listener, times(2)).onNotServing(any(StatusException.class));
    assertThat(sslSupplier0.isShutdown()).isFalse();

    when(mockServer.start()).thenThrow(new IOException("error!"))
            .thenReturn(mockServer);
    FilterChain filterChain1 = createFilterChain("filter-chain-1", createRds("rds"));
    SslContextProviderSupplier sslSupplier1 = filterChain1.sslContextProviderSupplier();
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain1), null);
    assertThat(sslSupplier0.isShutdown()).isTrue();
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    try {
      start.get(5000, TimeUnit.MILLISECONDS);
      fail("Start should throw exception");
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IOException.class);
      assertThat(ex.getCause().getMessage()).isEqualTo("error!");
    }
    assertThat(executor.forwardNanos(RETRY_DELAY_NANOS)).isEqualTo(1);
    verify(mockBuilder, times(1)).build();
    verify(mockServer, times(2)).start();
    verify(listener, times(1)).onServing();
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    ServerRoutingConfig realConfig =
        selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().get(filterChain1).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    // xds update after start
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-2")));
    assertThat(sslSupplier1.isShutdown()).isFalse();
    xdsClient.ldsWatcher.onAmbientError(Status.DEADLINE_EXCEEDED);
    verify(mockBuilder, times(1)).build();
    verify(mockServer, times(2)).start();
    verify(listener, times(2)).onNotServing(any(StatusException.class));
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs()
        .get(filterChain1).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-2")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    assertThat(sslSupplier1.isShutdown()).isFalse();

    // not serving after serving
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    assertThat(xdsClient.rdsWatchers).isEmpty();
    verify(mockServer, times(3)).shutdown(); // This is the 3rd shutdown in the test.
    when(mockServer.isShutdown()).thenReturn(true);
    assertThat(selectorManager.getSelectorToUpdateSelector())
        .isSameInstanceAs(FilterChainSelector.NO_FILTER_CHAIN);
    verify(listener, times(3)).onNotServing(any(StatusException.class));
    assertThat(sslSupplier1.isShutdown()).isTrue();
    assertThat(xdsClient.rdsWatchers.get("rds")).isNull();
    // no op
    saveRdsWatcher.onResourceChanged(StatusOr.fromValue(
        new RdsUpdate(Collections.singletonList(createVirtualHost("virtual-host-1")))));
    verify(mockBuilder, times(1)).build();
    verify(mockServer, times(2)).start();
    verify(listener, times(1)).onServing();

    // cancel retry
    when(mockServer.start()).thenThrow(new IOException("error1!"))
            .thenThrow(new IOException("error2!"))
            .thenReturn(mockServer);
    FilterChain filterChain2 = createFilterChain("filter-chain-2", createRds("rds"));
    SslContextProviderSupplier sslSupplier2 = filterChain2.sslContextProviderSupplier();
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain2), null);
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(sslSupplier1.isShutdown()).isTrue();
    verify(mockBuilder, times(2)).build();
    when(mockServer.isShutdown()).thenReturn(false);
    verify(mockServer, times(3)).start();
    verify(listener, times(1)).onServing();
    verify(listener, times(3)).onNotServing(any(StatusException.class));
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs()
        .get(filterChain2).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    assertThat(executor.numPendingTasks()).isEqualTo(1);
    xdsClient.ldsWatcher.onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    verify(mockServer, times(4)).shutdown();
    verify(listener, times(4)).onNotServing(any(StatusException.class));
    verify(listener, times(1)).onNotServing(any(IOException.class));
    when(mockServer.isShutdown()).thenReturn(true);
    assertThat(executor.numPendingTasks()).isEqualTo(0);
    assertThat(sslSupplier2.isShutdown()).isTrue();

    // serving after not serving
    FilterChain filterChain3 = createFilterChain("filter-chain-2", createRds("rds"));
    SslContextProviderSupplier sslSupplier3 = filterChain3.sslContextProviderSupplier();
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain3), null);
    xdsClient.deliverRdsUpdate("rds",
            Collections.singletonList(createVirtualHost("virtual-host-1")));
    verify(mockBuilder, times(3)).build();
    verify(mockServer, times(4)).start();
    verify(listener, times(1)).onServing();
    when(mockServer.isShutdown()).thenReturn(false);
    verify(listener, times(4)).onNotServing(any(StatusException.class));

    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    realConfig = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs()
        .get(filterChain3).get();
    assertThat(realConfig.virtualHosts()).isEqualTo(
        Collections.singletonList(createVirtualHost("virtual-host-1")));
    assertThat(realConfig.interceptors()).isEqualTo(ImmutableMap.of());

    xdsServerWrapper.shutdown();
    verify(mockServer, times(5)).shutdown();
    assertThat(sslSupplier3.isShutdown()).isTrue();
    when(mockServer.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
    assertThat(xdsServerWrapper.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interceptor_success() throws Exception {
    ArgumentCaptor<ConfigApplyingInterceptor> interceptorCaptor =
        ArgumentCaptor.forClass(ConfigApplyingInterceptor.class);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    verify(mockBuilder).intercept(interceptorCaptor.capture());
    ConfigApplyingInterceptor interceptor = interceptorCaptor.getValue();
    RouteMatch routeMatch =
        RouteMatch.create(
            PathMatcher.fromPath("/FooService/barMethod", true),
            Collections.<HeaderMatcher>emptyList(), null);
    Route route = Route.forAction(routeMatch, null,
        ImmutableMap.<String, FilterConfig>of());
    VirtualHost virtualHost  = VirtualHost.create(
        "v1", Collections.singletonList("foo.google.com"), Arrays.asList(route),
        ImmutableMap.<String, FilterConfig>of());
    final List<Integer> interceptorTrace = new ArrayList<>();
    ServerInterceptor interceptor0 = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        interceptorTrace.add(0);
        return next.startCall(call, headers);
      }
    };
    ServerRoutingConfig realConfig = ServerRoutingConfig.create(
        ImmutableList.of(virtualHost), ImmutableMap.of(route, interceptor0));
    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    when(serverCall.getMethodDescriptor()).thenReturn(createMethod("FooService/barMethod"));
    when(serverCall.getAttributes()).thenReturn(
        Attributes.newBuilder().set(ATTR_SERVER_ROUTING_CONFIG,
            new AtomicReference<>(realConfig)).build());
    when(serverCall.getAuthority()).thenReturn("foo.google.com");
    ServerCallHandler<Void, Void> next = mock(ServerCallHandler.class);
    interceptor.interceptCall(serverCall, new Metadata(), next);
    verify(next).startCall(eq(serverCall), any(Metadata.class));
    assertThat(interceptorTrace).isEqualTo(Arrays.asList(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interceptor_virtualHostNotMatch() throws Exception {
    ArgumentCaptor<ConfigApplyingInterceptor> interceptorCaptor =
            ArgumentCaptor.forClass(ConfigApplyingInterceptor.class);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    verify(mockBuilder).intercept(interceptorCaptor.capture());
    ConfigApplyingInterceptor interceptor = interceptorCaptor.getValue();
    ServerRoutingConfig routingConfig =
        createRoutingConfig("/FooService/barMethod", "foo.google.com");
    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    when(serverCall.getAttributes()).thenReturn(
        Attributes.newBuilder().set(ATTR_SERVER_ROUTING_CONFIG,
            new AtomicReference<>(routingConfig)).build());
    when(serverCall.getAuthority()).thenReturn("not-match.google.com");

    Filter.Provider filterProvider = mock(Filter.Provider.class);
    when(filterProvider.typeUrls()).thenReturn(new String[]{"filter-type-url"});
    when(filterProvider.isServerFilter()).thenReturn(true);
    filterRegistry.register(filterProvider);

    ServerCallHandler<Void, Void> next = mock(ServerCallHandler.class);
    interceptor.interceptCall(serverCall, new Metadata(), next);
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
    Status status = statusCaptor.getValue();
    assertThat(status.getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
    assertThat(status.getDescription()).isEqualTo("Could not find xDS virtual host matching RPC");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interceptor_routeNotMatch() throws Exception {
    ArgumentCaptor<ConfigApplyingInterceptor> interceptorCaptor =
            ArgumentCaptor.forClass(ConfigApplyingInterceptor.class);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    verify(mockBuilder).intercept(interceptorCaptor.capture());
    ConfigApplyingInterceptor interceptor = interceptorCaptor.getValue();
    ServerRoutingConfig routingConfig =
        createRoutingConfig("/FooService/barMethod", "foo.google.com");
    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    when(serverCall.getAttributes()).thenReturn(
            Attributes.newBuilder()
                .set(ATTR_SERVER_ROUTING_CONFIG, new AtomicReference<>(routingConfig)).build());
    when(serverCall.getMethodDescriptor()).thenReturn(createMethod("NotMatchMethod"));
    when(serverCall.getAuthority()).thenReturn("foo.google.com");

    Filter.Provider filterProvider = mock(Filter.Provider.class);
    when(filterProvider.typeUrls()).thenReturn(new String[]{"filter-type-url"});
    when(filterProvider.isServerFilter()).thenReturn(true);
    filterRegistry.register(filterProvider);

    ServerCallHandler<Void, Void> next = mock(ServerCallHandler.class);
    interceptor.interceptCall(serverCall, new Metadata(), next);
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
    Status status = statusCaptor.getValue();
    assertThat(status.getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
    assertThat(status.getDescription()).isEqualTo("Could not find xDS route matching RPC");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interceptor_invalidRouteAction() throws Exception {
    ArgumentCaptor<ConfigApplyingInterceptor> interceptorCaptor =
        ArgumentCaptor.forClass(ConfigApplyingInterceptor.class);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    verify(mockBuilder).intercept(interceptorCaptor.capture());
    ConfigApplyingInterceptor interceptor = interceptorCaptor.getValue();
    ServerRoutingConfig routingConfig =
        createRoutingConfig(
            "/FooService/barMethod",
            "foo.google.com",
            Route.RouteAction.forCluster(
                "cluster", Collections.<Route.RouteAction.HashPolicy>emptyList(), null, null,
                false));
    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    when(serverCall.getAttributes()).thenReturn(
        Attributes.newBuilder()
            .set(ATTR_SERVER_ROUTING_CONFIG, new AtomicReference<>(routingConfig)).build());
    when(serverCall.getMethodDescriptor()).thenReturn(createMethod("FooService/barMethod"));
    when(serverCall.getAuthority()).thenReturn("foo.google.com");

    Filter.Provider filterProvider = mock(Filter.Provider.class);
    when(filterProvider.typeUrls()).thenReturn(new String[]{"filter-type-url"});
    when(filterProvider.isServerFilter()).thenReturn(true);
    filterRegistry.register(filterProvider);

    ServerCallHandler<Void, Void> next = mock(ServerCallHandler.class);
    interceptor.interceptCall(serverCall, new Metadata(), next);
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
    Status status = statusCaptor.getValue();
    assertThat(status.getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
    assertThat(status.getDescription()).isEqualTo("Invalid xDS route action for matching "
        + "route: only Route.non_forwarding_action should be allowed.");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void interceptor_failingRouterConfig() throws Exception {
    ArgumentCaptor<ConfigApplyingInterceptor> interceptorCaptor =
            ArgumentCaptor.forClass(ConfigApplyingInterceptor.class);
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    verify(mockBuilder).intercept(interceptorCaptor.capture());
    ConfigApplyingInterceptor interceptor = interceptorCaptor.getValue();
    ServerCall<Void, Void> serverCall = mock(ServerCall.class);

    when(serverCall.getAttributes()).thenReturn(
        Attributes.newBuilder().set(ATTR_SERVER_ROUTING_CONFIG,
            new AtomicReference<>(ServerRoutingConfig.FAILING_ROUTING_CONFIG)).build());

    ServerCallHandler<Void, Void> next = mock(ServerCallHandler.class);
    interceptor.interceptCall(serverCall, new Metadata(), next);
    verify(next, never()).startCall(any(ServerCall.class), any(Metadata.class));
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(serverCall).close(statusCaptor.capture(), any(Metadata.class));
    Status status = statusCaptor.getValue();
    assertThat(status.getCode()).isEqualTo(Status.UNAVAILABLE.getCode());
    assertThat(status.getDescription()).isEqualTo(
        "Missing or broken xDS routing config: RDS config unavailable.");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void buildInterceptor_inline() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);
    RouteMatch routeMatch =
        RouteMatch.create(
            PathMatcher.fromPath("/FooService/barMethod", true),
            Collections.<HeaderMatcher>emptyList(), null);

    Filter filter = mock(Filter.class);
    Filter.Provider filterProvider = mock(Filter.Provider.class);
    when(filterProvider.typeUrls()).thenReturn(new String[]{"filter-type-url"});
    when(filterProvider.isServerFilter()).thenReturn(true);
    when(filterProvider.newInstance(any(FilterContext.class))).thenReturn(filter);
    filterRegistry.register(filterProvider);

    FilterConfig f0 = mock(FilterConfig.class);
    FilterConfig f0Override = mock(FilterConfig.class);
    when(f0.typeUrl()).thenReturn("filter-type-url");
    final List<Integer> interceptorTrace = new ArrayList<>();
    ServerInterceptor interceptor0 = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        interceptorTrace.add(0);
        return next.startCall(call, headers);
      }
    };
    ServerInterceptor interceptor1 = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        interceptorTrace.add(1);
        return next.startCall(call, headers);
      }
    };
    when(filter.buildServerInterceptor(f0, null)).thenReturn(interceptor0);
    when(filter.buildServerInterceptor(f0, f0Override)).thenReturn(interceptor1);
    Route route = Route.forAction(routeMatch, null,
        ImmutableMap.<String, FilterConfig>of());
    VirtualHost virtualHost  = VirtualHost.create(
        "v1", Collections.singletonList("foo.google.com"), Arrays.asList(route),
        ImmutableMap.of("filter-config-name-0", f0Override));
    HttpConnectionManager hcmVirtual = HttpConnectionManager.forVirtualHosts(
        0L, Collections.singletonList(virtualHost),
        Arrays.asList(new NamedFilterConfig("filter-config-name-0", f0),
            new NamedFilterConfig("filter-config-name-1", f0)));
    EnvoyServerProtoData.FilterChain filterChain = createFilterChain("filter-chain-0", hcmVirtual);
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    start.get(5000, TimeUnit.MILLISECONDS);
    verify(mockServer).start();
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    ServerInterceptor realInterceptor = selectorManager.getSelectorToUpdateSelector()
        .getRoutingConfigs().get(filterChain).get().interceptors().get(route);
    assertThat(realInterceptor).isNotNull();

    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    ServerCallHandler<Void, Void> mockNext = mock(ServerCallHandler.class);
    final ServerCall.Listener<Void> listener = new ServerCall.Listener<Void>() {};
    when(mockNext.startCall(any(ServerCall.class), any(Metadata.class))).thenReturn(listener);
    realInterceptor.interceptCall(serverCall, new Metadata(), mockNext);
    assertThat(interceptorTrace).isEqualTo(Arrays.asList(1, 0));
    verify(mockNext).startCall(eq(serverCall), any(Metadata.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void buildInterceptor_rds() throws Exception {
    final SettableFuture<Server> start = SettableFuture.create();
    Executors.newSingleThreadExecutor().execute(new Runnable() {
      @Override
      public void run() {
        try {
          start.set(xdsServerWrapper.start());
        } catch (Exception ex) {
          start.setException(ex);
        }
      }
    });
    xdsClient.ldsResource.get(5, TimeUnit.SECONDS);

    Filter filter = mock(Filter.class);
    Filter.Provider filterProvider = mock(Filter.Provider.class);
    when(filterProvider.typeUrls()).thenReturn(new String[]{"filter-type-url"});
    when(filterProvider.isServerFilter()).thenReturn(true);
    when(filterProvider.newInstance(any(FilterContext.class))).thenReturn(filter);
    filterRegistry.register(filterProvider);

    FilterConfig f0 = mock(FilterConfig.class);
    FilterConfig f0Override = mock(FilterConfig.class);
    when(f0.typeUrl()).thenReturn("filter-type-url");
    final List<Integer> interceptorTrace = new ArrayList<>();
    ServerInterceptor interceptor0 = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        interceptorTrace.add(0);
        return next.startCall(call, headers);
      }
    };
    ServerInterceptor interceptor1 = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
          Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        interceptorTrace.add(1);
        return next.startCall(call, headers);
      }
    };
    when(filter.buildServerInterceptor(f0, null)).thenReturn(interceptor0);
    when(filter.buildServerInterceptor(f0, f0Override)).thenReturn(interceptor1);
    RouteMatch routeMatch =
        RouteMatch.create(
            PathMatcher.fromPath("/FooService/barMethod", true),
            Collections.<HeaderMatcher>emptyList(), null);

    HttpConnectionManager rdsHcm = HttpConnectionManager.forRdsName(0L, "r0",
        Arrays.asList(new NamedFilterConfig("filter-config-name-0", f0),
            new NamedFilterConfig("filter-config-name-1", f0)));
    EnvoyServerProtoData.FilterChain filterChain = createFilterChain("filter-chain-0", rdsHcm);
    xdsClient.deliverLdsUpdate(Collections.singletonList(filterChain), null);
    Route route = Route.forAction(routeMatch, null,
        ImmutableMap.<String, FilterConfig>of());
    VirtualHost virtualHost  = VirtualHost.create(
        "v1", Collections.singletonList("foo.google.com"), Arrays.asList(route),
        ImmutableMap.of("filter-config-name-0", f0Override));
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("r0", Collections.singletonList(virtualHost));
    start.get(5000, TimeUnit.MILLISECONDS);
    verify(mockServer).start();
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs().size())
        .isEqualTo(1);
    ServerInterceptor realInterceptor = selectorManager.getSelectorToUpdateSelector()
        .getRoutingConfigs().get(filterChain).get().interceptors().get(route);
    assertThat(realInterceptor).isNotNull();

    ServerCall<Void, Void> serverCall = mock(ServerCall.class);
    ServerCallHandler<Void, Void> mockNext = mock(ServerCallHandler.class);
    final ServerCall.Listener<Void> listener = new ServerCall.Listener<Void>() {};
    when(mockNext.startCall(any(ServerCall.class), any(Metadata.class))).thenReturn(listener);
    realInterceptor.interceptCall(serverCall, new Metadata(), mockNext);
    assertThat(interceptorTrace).isEqualTo(Arrays.asList(1, 0));
    verify(mockNext).startCall(eq(serverCall), any(Metadata.class));

    virtualHost  = VirtualHost.create(
        "v1", Collections.singletonList("foo.google.com"), Arrays.asList(route),
         ImmutableMap.<String, FilterConfig>of());
    xdsClient.deliverRdsUpdate("r0", Collections.singletonList(virtualHost));
    realInterceptor = selectorManager.getSelectorToUpdateSelector().getRoutingConfigs()
        .get(filterChain).get().interceptors().get(route);
    assertThat(realInterceptor).isNotNull();
    interceptorTrace.clear();
    realInterceptor.interceptCall(serverCall, new Metadata(), mockNext);
    assertThat(interceptorTrace).isEqualTo(Arrays.asList(0, 0));
    verify(mockNext, times(2)).startCall(eq(serverCall), any(Metadata.class));

    Status notFoundStatus = Status.NOT_FOUND.withDescription("Resource r0 does not exist");
    xdsClient.rdsWatchers.get("r0").onResourceChanged(StatusOr.fromStatus(notFoundStatus));
    assertThat(selectorManager.getSelectorToUpdateSelector().getRoutingConfigs()
        .get(filterChain).get()).isEqualTo(noopConfig);
  }

  // Begin filter state tests.

  @Test
  public void filterState_survivesLds() {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    VirtualHost vhost = filterStateTestVhost();

    // LDS 1.
    FilterChain lds1FilterChain = createFilterChain("chain_0",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)));
    xdsClient.deliverLdsUpdate(lds1FilterChain, null);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    // Verify that StatefulFilter with different filter names result in different Filter instances.
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(2);
    // Naming: lds<LDS#>Filter<name#>
    StatefulFilter lds1Filter1 = lds1Snapshot.get(0);
    StatefulFilter lds1Filter2 = lds1Snapshot.get(1);
    assertThat(lds1Filter1).isNotSameInstanceAs(lds1Filter2);
    // Redundant check just in case StatefulFilter synchronization is broken.
    assertThat(lds1Filter1.idx).isEqualTo(0);
    assertThat(lds1Filter2.idx).isEqualTo(1);

    // LDS 2: filter configs with the same names.
    FilterChain lds2FilterChain = createFilterChain("chain_0",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)));
    xdsClient.deliverLdsUpdate(lds2FilterChain, null);
    ImmutableList<StatefulFilter> lds2Snapshot = statefulFilterProvider.getAllInstances();
    // Filter names hasn't changed, so expecting no new StatefulFilter instances.
    assertWithMessage("LDS 2: Expected Filter instances to be reused across LDS updates")
        .that(lds2Snapshot).isEqualTo(lds1Snapshot);

    // LDS 3: Filter "STATEFUL_2" removed.
    FilterChain lds3FilterChain = createFilterChain("chain_0",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1)));
    xdsClient.deliverLdsUpdate(lds3FilterChain, null);
    ImmutableList<StatefulFilter> lds3Snapshot = statefulFilterProvider.getAllInstances();
    // Again, no new StatefulFilter instances should be created.
    assertWithMessage("LDS 3: Expected Filter instances to be reused across LDS updates")
        .that(lds3Snapshot).isEqualTo(lds1Snapshot);
    // Verify the shutdown state.
    assertThat(lds1Filter1.isShutdown()).isFalse();
    assertWithMessage("LDS 3: Expected %s to be shut down", lds1Filter2)
        .that(lds1Filter2.isShutdown()).isTrue();

    // LDS 4: Filter "STATEFUL_2" added back.
    FilterChain lds4FilterChain = createFilterChain("chain_0",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)));
    xdsClient.deliverLdsUpdate(lds4FilterChain, null);
    ImmutableList<StatefulFilter> lds4Snapshot = statefulFilterProvider.getAllInstances();
    // Filter "STATEFUL_2" should be treated as any other new filter name in an LDS update:
    // a new instance should be created.
    assertWithMessage("LDS 4: Expected a new filter instance for %s", STATEFUL_2)
        .that(lds4Snapshot).hasSize(3);
    StatefulFilter lds4Filter2 = lds4Snapshot.get(2);
    assertThat(lds4Filter2.idx).isEqualTo(2);
    assertThat(lds4Filter2).isNotSameInstanceAs(lds1Filter2);
    assertThat(lds4Snapshot).containsAtLeastElementsIn(lds1Snapshot);
    // Verify the shutdown state.
    assertThat(lds1Filter1.isShutdown()).isFalse();
    assertThat(lds1Filter2.isShutdown()).isTrue();
    assertThat(lds4Filter2.isShutdown()).isFalse();
  }

  @Test
  public void filterState_survivesRds() throws Exception {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    String rdsName = "rds.example.com";

    // LDS 1.
    FilterChain fc1 = createFilterChain("fc1",
        createHcmForRds(rdsName, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)));
    xdsClient.deliverLdsUpdate(fc1, null);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    verify(listener, never()).onServing();
    // Server didn't start, but filter instances should have already been created.
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(2);
    // Naming: lds<LDS#>Filter<name#>
    StatefulFilter lds1Filter1 = lds1Snapshot.get(0);
    StatefulFilter lds1Filter2 = lds1Snapshot.get(1);
    assertThat(lds1Filter1).isNotSameInstanceAs(lds1Filter2);

    // RDS 1.
    VirtualHost vhost1 = filterStateTestVhost();
    xdsClient.deliverRdsUpdate(rdsName, vhost1);
    verifyServerStarted(serverStart);
    assertThat(getSelectorRoutingConfigs()).hasSize(1);
    assertThat(getSelectorVhosts(fc1)).containsExactly(vhost1);
    // Initial RDS update should not generate Filter instances.
    ImmutableList<StatefulFilter> rds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("RDS 1: Expected Filter instances to be reused across RDS route updates")
        .that(rds1Snapshot).isEqualTo(lds1Snapshot);

    // RDS 2: exactly the same as RDS 1.
    xdsClient.deliverRdsUpdate(rdsName, vhost1);
    assertThat(getSelectorRoutingConfigs()).hasSize(1);
    assertThat(getSelectorVhosts(fc1)).containsExactly(vhost1);
    ImmutableList<StatefulFilter> rds2Snapshot = statefulFilterProvider.getAllInstances();
    // Neither should any subsequent RDS updates.
    assertWithMessage("RDS 2: Expected Filter instances to be reused across RDS route updates")
        .that(rds2Snapshot).isEqualTo(lds1Snapshot);

    // RDS 3: Contains a per-route override for STATEFUL_1.
    VirtualHost vhost3 = filterStateTestVhost(vhost1.name(), ImmutableMap.of(
        STATEFUL_1, new Config("RDS3")
    ));
    xdsClient.deliverRdsUpdate(rdsName, vhost3);
    assertThat(getSelectorRoutingConfigs()).hasSize(1);
    assertThat(getSelectorVhosts(fc1)).containsExactly(vhost3);
    ImmutableList<StatefulFilter> rds3Snapshot = statefulFilterProvider.getAllInstances();
    // As with any other Route update, typed_per_filter_config overrides should not result in
    // creating new filter instances.
    assertWithMessage("RDS 3: Expected Filter instances to be reused on per-route filter overrides")
        .that(rds3Snapshot).isEqualTo(lds1Snapshot);
  }

  @Test
  public void filterState_uniquePerFilterChain() {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    // Prepare multiple filter chains matchers for testing.
    FilterChainMatch matcherA = createMatchSrcIp("3fff:a::/32");
    FilterChainMatch matcherB = createMatchSrcIp("3fff:b::/32");

    // Vhosts won't change too.
    VirtualHost vhostA = filterStateTestVhost("stateful_vhost_a");
    VirtualHost vhostB = filterStateTestVhost("stateful_vhost_b");

    // LDS 1.
    FilterChain lds1ChainA = createFilterChain("chain_a",
        createHcm(vhostA, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)),
        matcherA);
    FilterChain lds1ChainB = createFilterChain("chain_b",
        createHcm(vhostB, filterStateTestConfigs(STATEFUL_2)),
        matcherB);

    xdsClient.deliverLdsUpdate(ImmutableList.of(lds1ChainA, lds1ChainB), null);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    // Verify that filter with name STATEFUL_2 produced separate instances unique per filter chain.
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(3);
    // Naming: lds<LDS#>Chain<name>Filter<name#>
    StatefulFilter lds1ChainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter lds1ChainAFilter2 = lds1Snapshot.get(1);
    StatefulFilter lds1ChainBFilter2 = lds1Snapshot.get(2);
    assertThat(lds1ChainAFilter2).isNotSameInstanceAs(lds1ChainBFilter2);

    // LDS 2: In chain B filter with name STATEFUL_1 is replaced STATEFUL_2.
    FilterChain lds2ChainA = createFilterChain("chain_a",
        createHcm(vhostA, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)),
        matcherA);
    FilterChain lds2ChainB = createFilterChain("chain_b",
        createHcm(vhostB, filterStateTestConfigs(STATEFUL_1)),
        matcherB);

    xdsClient.deliverLdsUpdate(ImmutableList.of(lds2ChainA, lds2ChainB), null);
    ImmutableList<StatefulFilter> lds2Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 2: expected a distinct instance of filter %s for Chain B", STATEFUL_1)
        .that(lds2Snapshot).hasSize(4);
    StatefulFilter lds2ChainBFilter1 = lds2Snapshot.get(3);
    assertThat(lds2ChainBFilter1).isNotSameInstanceAs(lds1ChainAFilter1);
    // Confirm correct STATEFUL_2 has been shut down.
    assertThat(lds1ChainBFilter2.isShutdown()).isTrue();
    assertThat(lds1ChainAFilter2.isShutdown()).isFalse();

    // LDS 3: Add default chain
    // Default filter chain is an exception from the uniqueness rule, and we need to make sure
    // that this is accounted for when we're tracking active filters per unique FilterChain.
    FilterChain lds3ChainDefault = createFilterChain("chain_default",
        createHcm(vhostA, filterStateTestConfigs(STATEFUL_1, STATEFUL_2)),
        matcherA);
    xdsClient.deliverLdsUpdate(ImmutableList.of(lds2ChainA, lds2ChainB), lds3ChainDefault);
    ImmutableList<StatefulFilter> lds3Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 3: Expected two new distinct filter instances for default chain")
        .that(lds3Snapshot).hasSize(6);
    StatefulFilter lds3ChainDefaultFilter1 = lds3Snapshot.get(4);
    StatefulFilter lds3ChainDefaultFilter2 = lds3Snapshot.get(5);
    // STATEFUL_1 in default chain not the same STATEFUL_1 in chain A or B
    assertThat(lds3ChainDefaultFilter1).isNotSameInstanceAs(lds1ChainAFilter1);
    assertThat(lds3ChainDefaultFilter1).isNotSameInstanceAs(lds2ChainBFilter1);
    // STATEFUL_2 in default chain not the same STATEFUL_1 in chain A
    assertThat(lds3ChainDefaultFilter2).isNotSameInstanceAs(lds1ChainAFilter2);
  }

  /**
   * Verifies a special case where an existing filter is has a different typeUrl in a subsequent
   * LDS update.
   *
   * <p>Expectations:
   *   1. The old filter instance must be shutdown.
   *   2. A new filter instance must be created for the new filter with different typeUrl.
   */
  @Test
  public void filterState_specialCase_sameNameDifferentTypeUrl() {
    // Setup the server with filter containing StatefulFilter.Provider for two distict type URLs.
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    String altTypeUrl = "type.googleapis.com/grpc.test.AltStatefulFilter";
    StatefulFilter.Provider altStatefulFilterProvider = new StatefulFilter.Provider(altTypeUrl);
    FilterRegistry filterRegistry = FilterRegistry.newRegistry()
        .register(statefulFilterProvider, altStatefulFilterProvider, ROUTER_FILTER_PROVIDER);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    // Test a normal chain and the default chain, as it's handled separately.
    VirtualHost vhost = filterStateTestVhost();

    // LDS 1.
    ImmutableList<NamedFilterConfig> lds1Confgs = filterStateTestConfigs(STATEFUL_1, STATEFUL_2);
    FilterChain lds1ChainA = createFilterChain("chain_a", createHcm(vhost, lds1Confgs));
    FilterChain lds1ChainDefault = createFilterChain("chain_default", createHcm(vhost, lds1Confgs));
    xdsClient.deliverLdsUpdate(lds1ChainA, lds1ChainDefault);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(4);
    // Naming: lds<LDS#>Chain<name>Filter<name#>
    StatefulFilter lds1ChainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter lds1ChainAFilter2 = lds1Snapshot.get(1);
    StatefulFilter lds1ChainDefaultFilter1 = lds1Snapshot.get(2);
    StatefulFilter lds1ChainDefaultFilter2 = lds1Snapshot.get(3);

    // LDS 2: Filter STATEFUL_2 present, but with a different typeUrl: altTypeUrl.
    ImmutableList<NamedFilterConfig> lds2Confgs = ImmutableList.of(
        new NamedFilterConfig(STATEFUL_1, new StatefulFilter.Config(STATEFUL_1)),
        new NamedFilterConfig(STATEFUL_2, new StatefulFilter.Config(STATEFUL_2, altTypeUrl)),
        new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG)
    );
    FilterChain lds2ChainA = createFilterChain("chain_a", createHcm(vhost, lds2Confgs));
    FilterChain lds2ChainDefault = createFilterChain("chain_default", createHcm(vhost, lds2Confgs));
    xdsClient.deliverLdsUpdate(lds2ChainA, lds2ChainDefault);
    ImmutableList<StatefulFilter> lds2Snapshot = statefulFilterProvider.getAllInstances();
    ImmutableList<StatefulFilter> lds2SnapshotAlt = altStatefulFilterProvider.getAllInstances();
    // Filter "STATEFUL_2" has different typeUrl, and should be treated as a new filter.
    // No changes in the snapshot of normal stateful filters.
    assertThat(lds2Snapshot).isEqualTo(lds1Snapshot);
    // Two new filter instances is created by altStatefulFilterProvider for chainA and chainDefault.
    assertWithMessage("LDS 2: expected new filter instances for type %s", altTypeUrl)
        .that(lds2SnapshotAlt).hasSize(2);
    StatefulFilter lds2ChainAFilter2Alt = lds2SnapshotAlt.get(0);
    StatefulFilter lds2ChainADefault2Alt = lds2SnapshotAlt.get(1);
    // Confirm two new distict instances of STATEFUL_2 were created.
    assertThat(lds2ChainAFilter2Alt).isNotSameInstanceAs(lds1ChainAFilter2);
    assertThat(lds2ChainADefault2Alt).isNotSameInstanceAs(lds1ChainDefaultFilter2);
    assertThat(lds2ChainAFilter2Alt).isNotSameInstanceAs(lds2ChainADefault2Alt);
    // Verify the instance of STATEFUL_2 of the old type are shutdown.
    assertThat(lds1ChainAFilter2.isShutdown()).isTrue();
    assertThat(lds1ChainDefaultFilter2.isShutdown()).isTrue();
    // Verify the new instances of STATEFUL_2 and the old instances of STATEFUL_1 are running.
    assertThat(lds2ChainAFilter2Alt.isShutdown()).isFalse();
    assertThat(lds2ChainADefault2Alt.isShutdown()).isFalse();
    assertThat(lds1ChainAFilter1.isShutdown()).isFalse();
    assertThat(lds1ChainDefaultFilter1.isShutdown()).isFalse();
  }

  /**
   * Verifies that all filter instances are shutdown (closed) on LDS resource not found.
   */
  @Test
  public void filterState_shutdown_onLdsNotFound() {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    // Test a normal chain and the default chain, as it's handled separately.
    VirtualHost vhost = filterStateTestVhost();
    FilterChain chainA = createFilterChain("chain_a",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1)));
    FilterChain chainDefault = createFilterChain("chain_default",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_2)));

    // LDS 1.
    xdsClient.deliverLdsUpdate(chainA, chainDefault);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(2);
    // Naming: lds<LDS#>Chain<name>Filter<name#>
    StatefulFilter lds1ChainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter lds1ChainDefaultFilter2 = lds1Snapshot.get(1);

    // LDS 2: resource not found.
    xdsClient.deliverLdsResourceNotFound();
    // Verify shutdown.
    assertThat(lds1ChainAFilter1.isShutdown()).isTrue();
    assertThat(lds1ChainDefaultFilter2.isShutdown()).isTrue();
  }

  /**
   * Verifies that all filter instances of a filter chain are shutdown when said chain is removed.
   */
  @Test
  public void filterState_shutdown_onChainRemoved() {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    ImmutableList<NamedFilterConfig> configs = filterStateTestConfigs(STATEFUL_1, STATEFUL_2);
    FilterChain chainA = createFilterChain("chain_a",
        createHcm(filterStateTestVhost("stateful_vhost_a"), configs),
        createMatchSrcIp("3fff:a::/32"));
    FilterChain chainB = createFilterChain("chain_b",
        createHcm(filterStateTestVhost("stateful_vhost_b"), configs),
        createMatchSrcIp("3fff:b::/32"));
    FilterChain chainDefault = createFilterChain("chain_default",
        createHcm(filterStateTestVhost("stateful_vhost_default"), configs),
        createMatchSrcIp("3fff:defa::/32"));

    // LDS 1.
    xdsClient.deliverLdsUpdate(ImmutableList.of(chainA, chainB), chainDefault);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(6);
    StatefulFilter chainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter chainAFilter2 = lds1Snapshot.get(1);
    StatefulFilter chainBFilter1 = lds1Snapshot.get(2);
    StatefulFilter chainBFilter2 = lds1Snapshot.get(3);
    StatefulFilter chainDefaultFilter1 = lds1Snapshot.get(4);
    StatefulFilter chainDefaultFilter2 = lds1Snapshot.get(5);

    // LDS 2: ChainB and ChainDefault are gone.
    xdsClient.deliverLdsUpdate(chainA, null);
    assertThat(statefulFilterProvider.getAllInstances()).isEqualTo(lds1Snapshot);
    // ChainA filters not shutdown (just in case).
    assertThat(chainAFilter1.isShutdown()).isFalse();
    assertThat(chainAFilter2.isShutdown()).isFalse();
    // ChainB and ChainDefault filters shutdown.
    assertWithMessage("chainBFilter1").that(chainBFilter1.isShutdown()).isTrue();
    assertWithMessage("chainBFilter2").that(chainBFilter2.isShutdown()).isTrue();
    assertWithMessage("chainDefaultFilter1").that(chainDefaultFilter1.isShutdown()).isTrue();
    assertWithMessage("chainDefaultFilter2").that(chainDefaultFilter2.isShutdown()).isTrue();
  }

  /**
   * Verifies that all filter instances are shutdown (closed) on LDS ResourceWatcher shutdown.
   */
  @Test
  public void filterState_shutdown_onServerShutdown() {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    // Test a normal chain and the default chain, as it's handled separately.
    VirtualHost vhost = filterStateTestVhost();
    FilterChain chainA = createFilterChain("chain_a",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_1)));
    FilterChain chainDefault = createFilterChain("chain_default",
        createHcm(vhost, filterStateTestConfigs(STATEFUL_2)));

    // LDS 1.
    xdsClient.deliverLdsUpdate(chainA, chainDefault);
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(2);
    // Naming: lds<LDS#>Chain<name>Filter<name#>
    StatefulFilter lds1ChainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter lds1ChainDefaultFilter2 = lds1Snapshot.get(1);

    // Shutdown.
    xdsServerWrapper.shutdown();
    assertThat(xdsServerWrapper.isShutdown()).isTrue();
    assertThat(xdsClient.isShutDown()).isTrue();
    // Verify shutdown.
    assertThat(lds1ChainAFilter1.isShutdown()).isTrue();
    assertThat(lds1ChainDefaultFilter2.isShutdown()).isTrue();
  }

  /**
   * Verifies that filter instances are NOT shutdown on RDS_RESOURCE_NAME not found.
   */
  @Test
  public void filterState_shutdown_noShutdownOnRdsNotFound() throws Exception {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    String rdsName = "rds.example.com";
    // Test a normal chain and the default chain, as it's handled separately.
    FilterChain chainA = createFilterChain("chain_a",
        createHcmForRds(rdsName, filterStateTestConfigs(STATEFUL_1)));
    FilterChain chainDefault = createFilterChain("chain_default",
        createHcmForRds(rdsName, filterStateTestConfigs(STATEFUL_2)));

    xdsClient.deliverLdsUpdate(chainA, chainDefault);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    verify(listener, never()).onServing();
    // Server didn't start, but filter instances should have already been created.
    ImmutableList<StatefulFilter> lds1Snapshot = statefulFilterProvider.getAllInstances();
    assertWithMessage("LDS 1: expected to create filter instances").that(lds1Snapshot).hasSize(2);
    // Naming: lds<LDS#>Chain<name>Filter<name#>
    StatefulFilter lds1ChainAFilter1 = lds1Snapshot.get(0);
    StatefulFilter lds1ChainDefaultFilter2 = lds1Snapshot.get(1);

    // RDS 1: Standard vhost with a route.
    xdsClient.deliverRdsUpdate(rdsName, filterStateTestVhost());
    verifyServerStarted(serverStart);
    assertThat(statefulFilterProvider.getAllInstances()).isEqualTo(lds1Snapshot);

    // RDS 2: RDS_RESOURCE_NAME not found.
    xdsClient.deliverRdsResourceNotFound(rdsName);
    assertThat(lds1ChainAFilter1.isShutdown()).isFalse();
    assertThat(lds1ChainDefaultFilter2.isShutdown()).isFalse();
  }

  /** An ambient (transient) RDS error is not a config change, so nothing may be shut down. */
  @Test
  public void filterState_noShutdown_onRdsAmbientError() throws Exception {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);

    String rdsName = "rds.example.com";
    xdsClient.deliverLdsUpdate(createFilterChain("chain_0",
        createHcmForRds(rdsName, filterStateTestConfigs(STATEFUL_1, STATEFUL_2))), null);
    VirtualHost vhost = filterStateTestVhost("stateful-vhost", ImmutableMap.of(
        STATEFUL_1, new StatefulFilter.Config("override")));
    xdsClient.deliverRdsUpdate(rdsName, ImmutableList.of(vhost));
    verifyServerStarted(serverStart);
    ImmutableList<StatefulFilter> all = statefulFilterProvider.getAllInstances();
    assertThat(all).hasSize(2);

    xdsClient.rdsWatchers.get(rdsName).onAmbientError(
        Status.UNAVAILABLE.withDescription("transient ADS error"));

    assertThat(all.get(0).isShutdown()).isFalse();
    assertThat(all.get(1).isShutdown()).isFalse();
  }

  /**
   * On a named chain and on the default chain alike: a nested filter reachable only through an
   * RDS override is created when the override arrives and closed, once, when a later RDS drops
   * it; nothing else is touched; and an RDS error, which builds nothing, closes nothing.
   */
  @Test
  public void filterState_nestedFilterOfRdsOverride_createdAndClosedWithIt() throws Exception {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    try {
      StatefulFilter.Provider statefulProvider = new StatefulFilter.Provider();
      SettableFuture<Server> serverStart =
          filterStateTestStartServerWithComposite(statefulProvider);
      String rdsName = "rds.example.com";
      CompositeFilter.CompositeFilterConfig nestingA =
          parseComposite(compositeAnyWrapping(executeStatefulChain("a")));

      xdsClient.deliverLdsUpdate(
          createFilterChain("chain_a", createHcmForRds(rdsName, ImmutableList.of(
              new NamedFilterConfig(STATEFUL_1, new StatefulFilter.Config(STATEFUL_1)),
              new NamedFilterConfig("c", nestingA),
              new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG)))),
          createFilterChain("chain_default", createHcmForRds(rdsName, ImmutableList.of(
              new NamedFilterConfig(STATEFUL_2, new StatefulFilter.Config(STATEFUL_2)),
              new NamedFilterConfig("c", nestingA),
              new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG)))));
      xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
      // Top-level filters exist before the routes arrive; nested ones only once routes are built.
      assertThat(statefulFilterNames(statefulProvider)).containsExactly(STATEFUL_1, STATEFUL_2);

      // RDS 1: two routes, no overrides.
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestTwoRouteVhost(NO_FILTER_OVERRIDES));
      verifyServerStarted(serverStart);
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly(STATEFUL_1, STATEFUL_2, "a", "a");
      ImmutableList<StatefulFilter> rds1Snapshot = statefulProvider.getAllInstances();

      // RDS 2: the second route overrides "c" with a matcher that runs "x" instead.
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestTwoRouteVhost(
          ImmutableMap.of("c", parseCompositeOverride(executeStatefulChain("x")))));
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly(STATEFUL_1, STATEFUL_2, "a", "a", "x", "x");
      ImmutableList<StatefulFilter> rds2Snapshot = statefulProvider.getAllInstances();
      for (StatefulFilter filter : rds2Snapshot) {
        assertWithMessage("%s", filter).that(filter.isShutdown()).isFalse();
      }

      // RDS 3: the override is gone, so each chain's "x" is unreachable and closed exactly once:
      // StatefulFilter throws on a second close, which the update would log at SEVERE.
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestTwoRouteVhost(NO_FILTER_OVERRIDES));
      assertThat(statefulProvider.getAllInstances()).isEqualTo(rds2Snapshot);
      for (StatefulFilter filter : rds1Snapshot) {
        assertWithMessage("%s", filter).that(filter.isShutdown()).isFalse();
      }
      assertThat(rds2Snapshot.get(4).isShutdown()).isTrue();
      assertThat(rds2Snapshot.get(5).isShutdown()).isTrue();
      assertThat(severeLogs).isEmpty();

      // RDS 4: resource not found. Nothing is built, so nothing is swept.
      xdsClient.deliverRdsResourceNotFound(rdsName);
      for (StatefulFilter filter : rds1Snapshot) {
        assertWithMessage("%s", filter).that(filter.isShutdown()).isFalse();
      }

      // RDS 5: the resource is back. The nested instances kept through the error are reused.
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestTwoRouteVhost(NO_FILTER_OVERRIDES));
      assertThat(statefulProvider.getAllInstances()).isEqualTo(rds2Snapshot);
      for (StatefulFilter filter : rds1Snapshot) {
        assertWithMessage("%s", filter).that(filter.isShutdown()).isFalse();
      }
      assertThat(severeLogs).isEmpty();
    } finally {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }
  }

  /**
   * While a new chain's RDS is pending, the selector still holds the previous LDS's chains, and a
   * re-sent RDS rebuilds their routing configs with the previous HCM's filter set. Such a stale
   * rebuild must not leave the chain without a filter the new LDS added, or the selector update
   * that follows the pending RDS fails.
   */
  @Test
  public void filterState_staleRdsRebuildWhileNewChainRdsPending_selectorUpdated()
      throws Exception {
    StatefulFilter.Provider statefulFilterProvider = new StatefulFilter.Provider();
    FilterRegistry filterRegistry = filterStateTestFilterRegistry(statefulFilterProvider);
    SettableFuture<Server> serverStart = filterStateTestStartServer(filterRegistry);
    VirtualHost vhost = filterStateTestVhost();
    FilterChainMatch matchA = createMatchSrcIp("3fff:a::/32");
    FilterChainMatch matchB = createMatchSrcIp("3fff:b::/32");

    // LDS 1: chain A with STATEFUL_1, routes from rds_a.
    xdsClient.deliverLdsUpdate(createFilterChain("chain_a",
        createHcmForRds("rds_a", filterStateTestConfigs(STATEFUL_1)), matchA), null);
    xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
    xdsClient.deliverRdsUpdate("rds_a", vhost);
    verifyServerStarted(serverStart);
    assertThat(statefulFilterNames(statefulFilterProvider)).containsExactly(STATEFUL_1);

    // LDS 2: chain A gains STATEFUL_2, and a new chain B is served by rds_b, which stays pending.
    FilterChain lds2ChainA = createFilterChain("chain_a",
        createHcmForRds("rds_a", filterStateTestConfigs(STATEFUL_1, STATEFUL_2)), matchA);
    FilterChain lds2ChainB = createFilterChain("chain_b",
        createHcmForRds("rds_b", filterStateTestConfigs(STATEFUL_1)), matchB);
    xdsClient.deliverLdsUpdate(ImmutableList.of(lds2ChainA, lds2ChainB), null);
    assertThat(statefulFilterNames(statefulFilterProvider))
        .containsExactly(STATEFUL_1, STATEFUL_2, STATEFUL_1).inOrder();

    // rds_a re-sent: rebuilds chain A's routing config as of LDS 1, then rds_b arrives.
    xdsClient.deliverRdsUpdate("rds_a", vhost);
    xdsClient.deliverRdsUpdate("rds_b", vhost);
    assertThat(severeLogs).isEmpty();
    assertThat(getSelectorRoutingConfigs().keySet()).containsExactly(lds2ChainA, lds2ChainB);
    assertThat(getSelectorVhosts(lds2ChainA)).containsExactly(vhost);
    assertThat(getSelectorVhosts(lds2ChainB)).containsExactly(vhost);
    // Chain A has a live STATEFUL_2 instance.
    List<StatefulFilter> liveStateful2 = new ArrayList<>();
    for (StatefulFilter filter : statefulFilterProvider.getAllInstances()) {
      if (STATEFUL_2.equals(filter.name) && !filter.isShutdown()) {
        liveStateful2.add(filter);
      }
    }
    assertThat(liveStateful2).hasSize(1);
  }

  /**
   * An RDS error closes nothing, but an LDS update processed while the chain's RDS is still
   * unavailable reconciles the chain to its top-level filters right away: what the LDS dropped
   * is closed, and so are the nested instances, which are recreated once RDS recovers.
   */
  @Test
  public void filterState_ldsWhileRdsNotFound_reconcilesToTopLevelFilters() throws Exception {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    try {
      StatefulFilter.Provider statefulProvider = new StatefulFilter.Provider();
      SettableFuture<Server> serverStart =
          filterStateTestStartServerWithComposite(statefulProvider);
      String rdsName = "rds.example.com";
      NamedFilterConfig nestingA = new NamedFilterConfig("c", parseComposite(
          compositeAnyWrapping(executeStatefulChain("a"))));
      NamedFilterConfig stateful1 =
          new NamedFilterConfig(STATEFUL_1, new StatefulFilter.Config(STATEFUL_1));
      NamedFilterConfig stateful2 =
          new NamedFilterConfig(STATEFUL_2, new StatefulFilter.Config(STATEFUL_2));
      NamedFilterConfig router =
          new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG);

      // LDS 1 + RDS 1: everything is created.
      xdsClient.deliverLdsUpdate(createFilterChain("chain_a", createHcmForRds(rdsName,
          ImmutableList.of(stateful1, stateful2, nestingA, router))), null);
      xdsClient.awaitRds(FakeXdsClient.DEFAULT_TIMEOUT);
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestVhost());
      verifyServerStarted(serverStart);
      ImmutableList<StatefulFilter> rds1Snapshot = statefulProvider.getAllInstances();
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly(STATEFUL_1, STATEFUL_2, "a").inOrder();

      // RDS 2: resource not found. Nothing is built, so nothing is swept.
      xdsClient.deliverRdsResourceNotFound(rdsName);
      for (StatefulFilter filter : rds1Snapshot) {
        assertWithMessage("%s", filter).that(filter.isShutdown()).isFalse();
      }

      // LDS 2: STATEFUL_2 dropped while RDS is still unavailable. The chain is reconciled to its
      // top-level filters: STATEFUL_2 is closed now, and so is the nested "a".
      xdsClient.deliverLdsUpdate(createFilterChain("chain_a", createHcmForRds(rdsName,
          ImmutableList.of(stateful1, nestingA, router))), null);
      assertThat(statefulProvider.getAllInstances()).isEqualTo(rds1Snapshot);
      assertThat(rds1Snapshot.get(0).isShutdown()).isFalse();
      assertThat(rds1Snapshot.get(1).isShutdown()).isTrue();
      assertThat(rds1Snapshot.get(2).isShutdown()).isTrue();

      // RDS 3: the resource is back. STATEFUL_1 is reused, "a" is recreated.
      xdsClient.deliverRdsUpdate(rdsName, filterStateTestVhost());
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly(STATEFUL_1, STATEFUL_2, "a", "a").inOrder();
      assertThat(rds1Snapshot.get(0).isShutdown()).isFalse();
      assertThat(statefulProvider.getAllInstances().get(3).isShutdown()).isFalse();
      assertThat(severeLogs).isEmpty();
    } finally {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }
  }

  /**
   * On a named chain and on the default chain alike: outer{inner{a,b}} -> outer{inner{a}} closes
   * b and reuses a; removing the chain closes its nested instances along with the rest.
   */
  @Test
  public void filterState_nestedComposite_dropsUnreachedGrandchild_chainRemovalClosesNested() {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    try {
      StatefulFilter.Provider statefulProvider = new StatefulFilter.Provider();
      SettableFuture<Server> serverStart =
          filterStateTestStartServerWithComposite(statefulProvider);
      VirtualHost vhost = filterStateTestVhost();

      // LDS 1.
      ImmutableList<NamedFilterConfig> lds1Configs = ImmutableList.of(
          new NamedFilterConfig("outer", parseComposite(compositeAnyWrapping(executeOnMatch(
              "inner", compositeAnyWrapping(executeStatefulChain("a", "b")))))),
          new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG));
      xdsClient.deliverLdsUpdate(
          createFilterChain("chain_a", createHcm(vhost, lds1Configs)),
          createFilterChain("chain_default", createHcm(vhost, lds1Configs)));
      verifyServerStarted(serverStart);
      ImmutableList<StatefulFilter> lds1Snapshot = statefulProvider.getAllInstances();
      // Naming: chain_a's a, b; then chain_default's a, b.
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly("a", "b", "a", "b").inOrder();

      // LDS 2: inner no longer runs b.
      ImmutableList<NamedFilterConfig> lds2Configs = ImmutableList.of(
          new NamedFilterConfig("outer", parseComposite(compositeAnyWrapping(executeOnMatch(
              "inner", compositeAnyWrapping(executeStatefulChain("a")))))),
          new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG));
      xdsClient.deliverLdsUpdate(
          createFilterChain("chain_a", createHcm(vhost, lds2Configs)),
          createFilterChain("chain_default", createHcm(vhost, lds2Configs)));
      assertThat(statefulProvider.getAllInstances()).isEqualTo(lds1Snapshot);
      assertThat(lds1Snapshot.get(0).isShutdown()).isFalse();
      assertThat(lds1Snapshot.get(1).isShutdown()).isTrue();
      assertThat(lds1Snapshot.get(2).isShutdown()).isFalse();
      assertThat(lds1Snapshot.get(3).isShutdown()).isTrue();

      // LDS 3: both chains are gone, replaced by one with no composite.
      xdsClient.deliverLdsUpdate(
          createFilterChain("chain_b", createHcm(vhost, filterStateTestConfigs(STATEFUL_1))), null);
      assertThat(statefulFilterNames(statefulProvider))
          .containsExactly("a", "b", "a", "b", STATEFUL_1).inOrder();
      assertThat(lds1Snapshot.get(0).isShutdown()).isTrue();
      assertThat(lds1Snapshot.get(2).isShutdown()).isTrue();
    } finally {
      System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    }
  }

  /**
   * Starts the server with a registry holding StatefulFilter, the composite filter and the router.
   * The composite resolves nested filter types through that same registry.
   */
  private SettableFuture<Server> filterStateTestStartServerWithComposite(
      StatefulFilter.Provider statefulProvider) {
    FilterRegistry filterRegistry = FilterRegistry.newRegistry();
    compositeProvider = new CompositeFilter.Provider(filterRegistry::get);
    filterRegistry.register(statefulProvider, compositeProvider, ROUTER_FILTER_PROVIDER);
    return filterStateTestStartServer(filterRegistry);
  }

  private static ImmutableList<String> statefulFilterNames(StatefulFilter.Provider provider) {
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (StatefulFilter f : provider.getAllInstances()) {
      names.add(f.name);
    }
    return names.build();
  }

  private CompositeFilter.CompositeFilterConfig parseComposite(Any compositeAny) {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        compositeProvider.parseFilterConfig(compositeAny, compositeParseContext());
    assertThat(result.errorDetail).isNull();
    return result.config;
  }

  /** Parses an ExtensionWithMatcherPerRoute override whose matcher always runs {@code onMatch}. */
  private CompositeFilter.CompositeFilterConfig parseCompositeOverride(
      com.github.xds.type.matcher.v3.Matcher.OnMatch onMatch) {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        compositeProvider.parseFilterConfigOverride(
            Any.pack(ExtensionWithMatcherPerRoute.newBuilder()
                .setXdsMatcher(matcherFallingThroughTo(onMatch))
                .build()),
            compositeParseContext());
    assertThat(result.errorDetail).isNull();
    return result.config;
  }

  /** An ExecuteFilterAction running the named StatefulFilters as a filter_chain, in order. */
  private static com.github.xds.type.matcher.v3.Matcher.OnMatch executeStatefulChain(
      String... nestedNames) {
    FilterChainConfiguration.Builder chain = FilterChainConfiguration.newBuilder();
    for (String name : nestedNames) {
      chain.addTypedConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
          .setName(name)
          .setTypedConfig(Any.newBuilder().setTypeUrl(StatefulFilter.DEFAULT_TYPE_URL).build()));
    }
    return com.github.xds.type.matcher.v3.Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_" + String.join("_", nestedNames))
            .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                .setFilterChain(chain)
                .build()))
            .build())
        .build();
  }

  /** Wraps {@code childAny} in an ExecuteFilterAction reached through a matcher's on_no_match. */
  private static com.github.xds.type.matcher.v3.Matcher.OnMatch executeOnMatch(
      String childName, Any childAny) {
    return com.github.xds.type.matcher.v3.Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_" + childName)
            .setTypedConfig(Any.pack(ExecuteFilterAction.newBuilder()
                .setTypedConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
                    .setName(childName)
                    .setTypedConfig(childAny)
                    .build())
                .build()))
            .build())
        .build();
  }

  private static Any compositeAnyWrapping(
      com.github.xds.type.matcher.v3.Matcher.OnMatch onMatch) {
    return Any.pack(ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .setXdsMatcher(matcherFallingThroughTo(onMatch))
        .build());
  }

  /**
   * A Matcher must have a matcher_list or matcher_tree (A106). This one's single field matcher
   * keys on a header no test sends, so {@code onNoMatch} is what always runs.
   */
  private static com.github.xds.type.matcher.v3.Matcher matcherFallingThroughTo(
      com.github.xds.type.matcher.v3.Matcher.OnMatch onNoMatch) {
    return com.github.xds.type.matcher.v3.Matcher.newBuilder()
        .setMatcherList(com.github.xds.type.matcher.v3.Matcher.MatcherList.newBuilder()
            .addMatchers(com.github.xds.type.matcher.v3.Matcher.MatcherList.FieldMatcher
                .newBuilder()
                .setPredicate(com.github.xds.type.matcher.v3.Matcher.MatcherList.Predicate
                    .newBuilder()
                    .setSinglePredicate(com.github.xds.type.matcher.v3.Matcher.MatcherList
                        .Predicate.SinglePredicate.newBuilder()
                        .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                            .setName("request_headers")
                            .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                                .setHeaderName("x-never-sent")
                                .build())))
                        .setValueMatch(com.github.xds.type.matcher.v3.StringMatcher.newBuilder()
                            .setExact("never"))))
                .setOnMatch(onNoMatch)))
        .setOnNoMatch(onNoMatch)
        .build();
  }

  private static Filter.FilterConfigParseContext compositeParseContext() {
    return Filter.FilterConfigParseContext.builder()
        .bootstrapInfo(Bootstrapper.BootstrapInfo.builder()
            .servers(Collections.singletonList(
                Bootstrapper.ServerInfo.create("test_target", Collections.emptyMap())))
            .node(EnvoyProtoData.Node.newBuilder().build())
            .build())
        .serverInfo(Bootstrapper.ServerInfo.create(
            "test_target", Collections.emptyMap(), false, true, false, false))
        .build();
  }

  private FilterRegistry filterStateTestFilterRegistry(
      StatefulFilter.Provider statefulFilterProvider) {
    return FilterRegistry.newRegistry().register(statefulFilterProvider, ROUTER_FILTER_PROVIDER);
  }

  private SettableFuture<Server> filterStateTestStartServer(FilterRegistry filterRegistry) {
    xdsServerWrapper = new XdsServerWrapper("0.0.0.0:1", mockBuilder, listener,
        selectorManager, new FakeXdsClientPoolFactory(xdsClient),
        XdsServerTestHelper.RAW_BOOTSTRAP, filterRegistry);
    SettableFuture<Server> serverStart = SettableFuture.create();
    scheduleServerStart(xdsServerWrapper, serverStart);
    return serverStart;
  }

  private static ImmutableList<NamedFilterConfig> filterStateTestConfigs(String... names) {
    ImmutableList.Builder<NamedFilterConfig> result = ImmutableList.builder();
    for (String name : names) {
      result.add(new NamedFilterConfig(name, new StatefulFilter.Config(name)));
    }
    result.add(new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG));
    return result.build();
  }

  private static Route filterStateTestRoute(ImmutableMap<String, FilterConfig> perRouteOverrides) {
    // Standard basic route for filterState tests.
    return Route.forAction(
        RouteMatch.withPathExactOnly("/grpc.test.HelloService/SayHello"), null, perRouteOverrides);
  }

  private static VirtualHost filterStateTestVhost() {
    return filterStateTestVhost("stateful-vhost", NO_FILTER_OVERRIDES);
  }

  private static VirtualHost filterStateTestVhost(String name) {
    return filterStateTestVhost(name, NO_FILTER_OVERRIDES);
  }

  private static VirtualHost filterStateTestVhost(
      String name, ImmutableMap<String, FilterConfig> perRouteOverrides) {
    return VirtualHost.create(
        name,
        ImmutableList.of("stateful.test.example.com"),
        ImmutableList.of(filterStateTestRoute(perRouteOverrides)),
        NO_FILTER_OVERRIDES);
  }

  /** A vhost with one route with no overrides and a second one with {@code route2Overrides}. */
  private static VirtualHost filterStateTestTwoRouteVhost(
      ImmutableMap<String, FilterConfig> route2Overrides) {
    return VirtualHost.create(
        "stateful-vhost",
        ImmutableList.of("stateful.test.example.com"),
        ImmutableList.of(
            filterStateTestRoute(NO_FILTER_OVERRIDES),
            Route.forAction(RouteMatch.withPathExactOnly("/grpc.test.HelloService/SayBye"), null,
                route2Overrides)),
        NO_FILTER_OVERRIDES);
  }

  // End filter state tests.

  private void verifyServerStarted(SettableFuture<Server> serverStart) {
    try {
      serverStart.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AssertionError("serverStart future failed to resolve within the timeout", e);
    }
    verify(listener).onServing();
    try {
      verify(mockServer).start();
    } catch (IOException e) {
      throw new AssertionError("mockServer.start() shouldn't throw", e);
    }
  }

  private Map<FilterChain, AtomicReference<ServerRoutingConfig>> getSelectorRoutingConfigs() {
    return selectorManager.getSelectorToUpdateSelector().getRoutingConfigs();
  }

  private ServerRoutingConfig getSelectorRoutingConfig(FilterChain fc) {
    return getSelectorRoutingConfigs().get(fc).get();
  }

  private ImmutableList<VirtualHost> getSelectorVhosts(FilterChain fc) {
    return getSelectorRoutingConfig(fc).virtualHosts();
  }

  public static void scheduleServerStart(
      XdsServerWrapper xdsServerWrapper, SettableFuture<Server> serverStart) {
    Executors.newSingleThreadExecutor().execute(() -> {
      try {
        serverStart.set(xdsServerWrapper.start());
      } catch (Exception e) {
        serverStart.setException(e);
      }
    });
  }

  private static FilterChain createFilterChain(String name, HttpConnectionManager hcm) {
    return createFilterChain(name, hcm, createMatch());
  }

  private static FilterChain createFilterChain(
      String name, HttpConnectionManager hcm, FilterChainMatch filterChainMatch) {
    TlsContextManager tlsContextManager = mock(TlsContextManager.class);
    return FilterChain.create(name, filterChainMatch, hcm, createTls(), tlsContextManager);
  }

  private static VirtualHost createVirtualHost(String name) {
    return VirtualHost.create(
            name, Collections.singletonList("auth"), new ArrayList<Route>(),
            ImmutableMap.<String, FilterConfig>of());
  }

  private static HttpConnectionManager createHcm(
      VirtualHost vhost, List<NamedFilterConfig> filterConfigs) {
    return HttpConnectionManager.forVirtualHosts(0L, ImmutableList.of(vhost), filterConfigs);
  }

  private static HttpConnectionManager createHcmForRds(
      String name, List<NamedFilterConfig> filterConfigs) {
    return HttpConnectionManager.forRdsName(0L, name, filterConfigs);
  }

  private static HttpConnectionManager createRds(String name) {
    NamedFilterConfig config =
        new NamedFilterConfig(ROUTER_FILTER_INSTANCE_NAME, RouterFilter.ROUTER_CONFIG);
    return createHcmForRds(name, ImmutableList.of(config));
  }

  /**
   * Returns the least-specific match-all Filter Chain Match.
   */
  static FilterChainMatch createMatch() {
    return FilterChainMatch.create(
        0,
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        EnvoyServerProtoData.ConnectionSourceType.ANY,
        ImmutableList.of(),
        ImmutableList.of(),
        "");
  }

  private static FilterChainMatch createMatchSrcIp(String srcCidr) {
    String[] srcParts = srcCidr.split("/", 2);
    InetAddress ip = InetAddresses.forString(srcParts[0]);
    Integer subnetMask = Integer.valueOf(srcParts[1], 10);
    return FilterChainMatch.create(
        0,
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(CidrRange.create(ip, subnetMask)),
        EnvoyServerProtoData.ConnectionSourceType.ANY,
        ImmutableList.of(),
        ImmutableList.of(),
        "");
  }

  private static ServerRoutingConfig createRoutingConfig(String path, String domain) {
    return createRoutingConfig(path, domain, null);
  }

  private static ServerRoutingConfig createRoutingConfig(
      String path, String domain, Route.RouteAction action) {
    RouteMatch routeMatch =
        RouteMatch.create(
            PathMatcher.fromPath(path, true),
            Collections.<HeaderMatcher>emptyList(), null);
    VirtualHost virtualHost  = VirtualHost.create(
        "v1", Collections.singletonList(domain),
        Arrays.asList(Route.forAction(routeMatch, action,
            ImmutableMap.<String, FilterConfig>of())),
        Collections.<String, FilterConfig>emptyMap());
    return ServerRoutingConfig.create(ImmutableList.<VirtualHost>of(virtualHost),
        ImmutableMap.<Route, ServerInterceptor>of()
    );
  }

  private static MethodDescriptor<Void, Void> createMethod(String path) {
    return MethodDescriptor.<Void, Void>newBuilder()
            .setType(MethodDescriptor.MethodType.UNKNOWN)
            .setFullMethodName(path)
            .setRequestMarshaller(TestMethodDescriptors.voidMarshaller())
            .setResponseMarshaller(TestMethodDescriptors.voidMarshaller())
            .build();
  }

  static EnvoyServerProtoData.DownstreamTlsContext createTls() {
    return CommonTlsContextTestsUtil.buildTestInternalDownstreamTlsContext("CERT1", "VA1");
  }

  @Test
  public void childChannelConfigurator_passedToXdsClientPool() {
    ChannelConfigurator configurator = builder -> { };
    XdsClientPoolFactory mockPoolFactory = mock(XdsClientPoolFactory.class);
    @SuppressWarnings("unchecked")
    ObjectPool<XdsClient> mockPool = mock(ObjectPool.class);
    when(mockPool.getObject()).thenReturn(xdsClient);
    when(mockPoolFactory.getOrCreate(any(), any(), any(), any())).thenReturn(mockPool);

    XdsServerWrapper serverWrapper = new XdsServerWrapper(
        "0.0.0.0:1", mockBuilder, listener, selectorManager, mockPoolFactory,
        XdsServerTestHelper.RAW_BOOTSTRAP, filterRegistry,
        executor.getScheduledExecutorService(), configurator);

    Executors.newSingleThreadExecutor().execute(() -> {
      try {
        serverWrapper.start();
      } catch (IOException ex) {
        // ignore
      }
    });

    verify(mockPoolFactory, timeout(5000)).getOrCreate(
        any(), any(), any(), eq(configurator));
    serverWrapper.shutdownNow();
  }
}
