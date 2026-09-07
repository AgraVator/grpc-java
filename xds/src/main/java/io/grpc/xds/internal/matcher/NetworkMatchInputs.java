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

package io.grpc.xds.internal.matcher;

import com.github.xds.core.v3.TypedExtensionConfig;
import io.grpc.Grpc;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLSession;
import javax.net.ssl.StandardConstants;

final class NetworkMatchInputs {
  private NetworkMatchInputs() {}

  static final class SourceIpInput implements MatchInput {
    static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.SourceIPInput";
    static final SourceIpInput INSTANCE = new SourceIpInput();

    @Override
    public String apply(MatchContext context) {
      SocketAddress addr = context.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
      if (addr instanceof InetSocketAddress) {
        InetSocketAddress inetAddr = (InetSocketAddress) addr;
        if (inetAddr.getAddress() != null) {
          return inetAddr.getAddress().getHostAddress();
        }
      }
      return null;
    }

    @Override
    public Class<?> outputType() {
      return String.class;
    }

    static final class Provider implements MatchInputProvider {
      @Override
      public MatchInput getInput(TypedExtensionConfig config) {
        return INSTANCE;
      }

      @Override
      public String typeUrl() {
        return TYPE_URL;
      }
    }
  }

  static final class SourcePortInput implements MatchInput {
    static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.SourcePortInput";
    static final SourcePortInput INSTANCE = new SourcePortInput();

    @Override
    public String apply(MatchContext context) {
      SocketAddress addr = context.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
      if (addr instanceof InetSocketAddress) {
        return String.valueOf(((InetSocketAddress) addr).getPort());
      }
      return null;
    }

    @Override
    public Class<?> outputType() {
      return String.class;
    }

    static final class Provider implements MatchInputProvider {
      @Override
      public MatchInput getInput(TypedExtensionConfig config) {
        return INSTANCE;
      }

      @Override
      public String typeUrl() {
        return TYPE_URL;
      }
    }
  }

  static final class DirectSourceIpInput implements MatchInput {
    static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3"
            + ".DirectSourceIPInput";
    static final DirectSourceIpInput INSTANCE = new DirectSourceIpInput();

    @Override
    public String apply(MatchContext context) {
      SocketAddress addr = context.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
      if (addr instanceof InetSocketAddress) {
        InetSocketAddress inetAddr = (InetSocketAddress) addr;
        if (inetAddr.getAddress() != null) {
          return inetAddr.getAddress().getHostAddress();
        }
      }
      return null;
    }

    @Override
    public Class<?> outputType() {
      return String.class;
    }

    static final class Provider implements MatchInputProvider {
      @Override
      public MatchInput getInput(TypedExtensionConfig config) {
        return INSTANCE;
      }

      @Override
      public String typeUrl() {
        return TYPE_URL;
      }
    }
  }

  static final class ServerNameInput implements MatchInput {
    static final String TYPE_URL =
        "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.ServerNameInput";
    static final ServerNameInput INSTANCE = new ServerNameInput();

    @Override
    public String apply(MatchContext context) {
      if (context.getCallOptions() != null && context.getCallOptions().getAuthority() != null) {
        return context.getCallOptions().getAuthority();
      }
      SSLSession session = context.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
      if (session instanceof javax.net.ssl.ExtendedSSLSession) {
        javax.net.ssl.ExtendedSSLSession extSession = (javax.net.ssl.ExtendedSSLSession) session;
        List<SNIServerName> names = extSession.getRequestedServerNames();
        if (names != null) {
          for (SNIServerName name : names) {
            if (name.getType() == StandardConstants.SNI_HOST_NAME && name instanceof SNIHostName) {
              return ((SNIHostName) name).getAsciiName();
            }
          }
        }
      }
      return null;
    }

    @Override
    public Class<?> outputType() {
      return String.class;
    }

    static final class Provider implements MatchInputProvider {
      @Override
      public MatchInput getInput(TypedExtensionConfig config) {
        return INSTANCE;
      }

      @Override
      public String typeUrl() {
        return TYPE_URL;
      }
    }
  }
}
