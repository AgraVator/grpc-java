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

import com.google.common.base.Preconditions;
import io.grpc.Metadata;
import javax.annotation.Nullable;

public final class MatchContext {
  private final Metadata metadata;
  @Nullable 
  private final String path;
  @Nullable 
  private final String host;
  @Nullable 
  private final String method;
  @Nullable 
  private final String id;
  private final io.grpc.Attributes attributes;
  @Nullable
  private final io.grpc.CallOptions callOptions;

  public MatchContext(Metadata metadata, @Nullable String path,
      @Nullable String host, @Nullable String method,
      @Nullable String id) {
    this(metadata, path, host, method, id, io.grpc.Attributes.EMPTY, null);
  }

  public MatchContext(Metadata metadata, @Nullable String path,
      @Nullable String host, @Nullable String method,
      @Nullable String id, io.grpc.Attributes attributes,
      @Nullable io.grpc.CallOptions callOptions) {
    this.metadata = Preconditions.checkNotNull(metadata, "metadata");
    this.path = path;
    this.host = host;
    this.method = method;
    this.id = id;
    this.attributes = attributes != null ? attributes : io.grpc.Attributes.EMPTY;
    this.callOptions = callOptions;
  }

  public Metadata getMetadata() {
    return metadata;
  }
  
  @Nullable
  public String getPath() {
    return path;
  }
  
  @Nullable
  public String getHost() {
    return host;
  }
  
  @Nullable
  public String getMethod() {
    return method;
  }
  
  @Nullable
  public String getId() {
    return id;
  }

  public io.grpc.Attributes getAttributes() {
    return attributes;
  }

  @Nullable
  public io.grpc.CallOptions getCallOptions() {
    return callOptions;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private Metadata metadata = new Metadata();
    private String path;
    private String host;
    private String method;
    private String id;
    private io.grpc.Attributes attributes = io.grpc.Attributes.EMPTY;
    private io.grpc.CallOptions callOptions;

    public Builder setMetadata(Metadata metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setMethod(String method) {
      this.method = method;
      return this;
    }

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setAttributes(io.grpc.Attributes attributes) {
      if (attributes != null) {
        this.attributes = attributes;
      }
      return this;
    }

    public Builder setCallOptions(io.grpc.CallOptions callOptions) {
      this.callOptions = callOptions;
      return this;
    }

    public MatchContext build() {
      return new MatchContext(metadata, path, host, method, id, attributes, callOptions);
    }
  }
}
