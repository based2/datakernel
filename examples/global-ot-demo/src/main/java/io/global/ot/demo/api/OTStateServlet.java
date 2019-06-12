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

package io.global.ot.demo.api;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import io.datakernel.exception.StacklessException;
import io.datakernel.http.HttpResponse;
import io.datakernel.http.RoutingServlet;
import io.datakernel.ot.OTRepository;
import io.datakernel.util.Tuple4;
import io.global.ot.api.CommitId;
import io.global.ot.demo.operations.Operation;
import io.global.ot.demo.operations.OperationState;
import io.global.ot.demo.util.ManagerProvider;

import static io.datakernel.codec.json.JsonUtils.fromJson;
import static io.datakernel.codec.json.JsonUtils.toJson;
import static io.datakernel.http.AsyncServletDecorator.loadBody;
import static io.datakernel.http.HttpMethod.GET;
import static io.datakernel.http.HttpMethod.POST;
import static io.datakernel.ot.OTAlgorithms.loadGraph;
import static io.global.ot.demo.util.Utils.*;
import static io.global.ot.graph.OTGraphServlet.COMMIT_ID_TO_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class OTStateServlet {
	private static final StacklessException MANAGER_NOT_INITIALIZED = new StacklessException(OTStateServlet.class, "Manager has not been initialized yet");

	private OTStateServlet() {
	}

	public static RoutingServlet create(ManagerProvider<Operation> provider, OTRepository<CommitId, Operation> repository) {
		return RoutingServlet.create()
				.with(GET, "/info", request -> getManager(provider, request)
						.then(manager -> {
							if (manager != null) {
								return repository
										.getHeads()
										.then(heads -> loadGraph(repository, provider.getSystem(), heads, COMMIT_ID_TO_STRING, DIFF_TO_STRING))
										.map(graph -> {
											String status = manager.hasPendingCommits() || manager.hasWorkingDiffs() ? "Syncing" : "Synced";
											Tuple4<CommitId, Integer, String, String> infoTuple = new Tuple4<>(
													manager.getCommitId(),
													((OperationState) manager.getState()).getCounter(),
													status,
													graph.toGraphViz(manager.getCommitId())
											);
											return okJson().withBody(toJson(INFO_CODEC, infoTuple).getBytes(UTF_8));
										});
							} else {
								return Promise.of(HttpResponse.redirect302("../?id=" + getNextId(provider)));
							}
						}))
				.with(POST, "/add", loadBody()
						.serve(request -> getManager(provider, request)
								.then(manager -> {
									ByteBuf body = request.getBody();
									if (manager != null) {
										try {
											Operation operation = fromJson(OPERATION_CODEC, body.getString(UTF_8));
											manager.add(operation);
											return manager.sync()
													.map($ -> okText());
										} catch (ParseException e) {
											return Promise.<HttpResponse>ofException(e);
										}
									} else {
										return Promise.<HttpResponse>ofException(MANAGER_NOT_INITIALIZED);
									}
								})));
	}
}
