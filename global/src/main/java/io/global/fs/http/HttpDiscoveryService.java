/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.global.fs.http;

import io.datakernel.async.Stage;
import io.datakernel.http.HttpException;
import io.datakernel.http.HttpMethod;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.IAsyncHttpClient;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import static io.global.fs.http.DiscoveryServlet.*;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HttpDiscoveryService implements DiscoveryService {
	private final IAsyncHttpClient client;
	private final InetSocketAddress address;

	private HttpDiscoveryService(InetSocketAddress address, IAsyncHttpClient client) {
		this.client = client;
		this.address = address;
	}

	public static HttpDiscoveryService create(InetSocketAddress address, IAsyncHttpClient client) {
		return new HttpDiscoveryService(address, client);
	}

	@Override
	public Stage<Void> announce(RepoID repo, SignedData<AnnounceData> announceData) {
		return client.request(
				HttpRequest.of(HttpMethod.PUT,
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(ANNOUNCE)
								.appendQuery("owner", repo.getOwner().asString())
								.appendQuery("name", repo.getName())
								.build())
						.withBody(SIGNED_ANNOUNCE.toJson(announceData).getBytes(UTF_8)))
				.thenCompose(response -> response.ensureStatusCode(201));
	}

	@Override
	public Stage<Optional<SignedData<AnnounceData>>> find(RepoID repo) {
		return client.requestWithResponseBody(Integer.MAX_VALUE,
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(FIND)
								.appendQuery("owner", repo.getOwner().asString())
								.appendQuery("name", repo.getName())
								.build()))
				.thenCompose(response -> {
					switch (response.getCode()) {
						case 200:
							try {
								return Stage.of(Optional.of(SIGNED_ANNOUNCE.fromJson(response.getBody().asString(UTF_8))));
							} catch (IOException e) {
								return Stage.ofException(e);
							}
						case 404:
							return Stage.of(Optional.empty());
						default:
							return Stage.ofException(HttpException.ofCode(response.getCode(), response.getBody().getString(UTF_8)));
					}
				});
	}

	@Override
	public Stage<List<SignedData<AnnounceData>>> find(PubKey owner) {
		return client.requestWithResponseBody(Integer.MAX_VALUE,
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(FIND_ALL)
								.appendQuery("owner", owner.asString())
								.build()))
				.thenCompose(response -> response.ensureStatusCode(200)
						.thenCompose($ -> {
							try {
								return Stage.of(LIST_OF_SIGNED_ANNOUNCES.fromJson(response.getBody().asString(UTF_8)));
							} catch (IOException e) {
								return Stage.ofException(e);
							}
						}));
	}

	@Override
	public Stage<Void> shareKey(PubKey owner, SignedData<SharedSimKey> simKey) {
		return client.request(
				HttpRequest.post(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(SHARE_KEY)
								.appendQuery("owner", owner.asString())
								.build())
						.withBody(SIGNED_SHARED_SIM_KEY.toJson(simKey).getBytes(UTF_8)))
				.thenCompose(response -> response.ensureStatusCode(201));
	}

	@Override
	public Stage<Optional<SignedData<SharedSimKey>>> getSharedKey(PubKey owner, PubKey receiver, Hash hash) {
		return client.requestWithResponseBody(Integer.MAX_VALUE,
				HttpRequest.get(
						UrlBuilder.http()
								.withAuthority(address)
								.appendPathPart(GET_SHARED_KEY)
								.appendQuery("owner", owner.asString())
								.appendQuery("receiver", receiver.asString())
								.appendQuery("hash", hash.asString())
								.build()))
				.thenCompose(response -> {
					switch (response.getCode()) {
						case 200:
							try {
								return Stage.of(Optional.of(SIGNED_SHARED_SIM_KEY.fromJson(response.getBody().asString(UTF_8))));
							} catch (IOException e) {
								return Stage.ofException(e);
							}
						case 404:
							return Stage.of(Optional.empty());
						default:
							return Stage.ofException(HttpException.ofCode(response.getCode(), response.getBody().getString(UTF_8)));
					}
				});
	}
}
