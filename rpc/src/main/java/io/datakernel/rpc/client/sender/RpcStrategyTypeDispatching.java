/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

public final class RpcStrategyTypeDispatching implements RpcRequestSendingStrategy, RpcSingleSenderStrategy {

	public enum Importance {
		MANDATORY, OPTIONAL
	}

	private Map<Class<? extends RpcMessage.RpcMessageData>, DataTypeSpecifications> dataTypeToSpecification;
	private RpcSingleSenderStrategy defaultSendingStrategy;

	RpcStrategyTypeDispatching() {
		dataTypeToSpecification = new HashMap<>();
		defaultSendingStrategy = null;
	}

	@Override
	public List<Optional<RpcRequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RpcRequestSender> create(RpcClientConnectionPool pool) {
		HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender = new HashMap<>();
		for (Class<? extends RpcMessage.RpcMessageData> dataType : dataTypeToSpecification.keySet()) {
			DataTypeSpecifications specs = dataTypeToSpecification.get(dataType);
			Optional<RpcRequestSender> sender = specs.getStrategy().create(pool);
			if (sender.isPresent()) {
				dataTypeToSender.put(dataType, sender.get());
			} else if (specs.getImportance() == Importance.MANDATORY) {
				return Optional.absent();
			}
		}
		Optional<RpcRequestSender> defaultSender =
				defaultSendingStrategy != null ? defaultSendingStrategy.create(pool) : Optional.<RpcRequestSender>absent();
		return Optional.<RpcRequestSender>of(new RequestSenderTypeDispatcher(dataTypeToSender, defaultSender.orNull()));
	}

	public RpcStrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                     RpcSingleSenderStrategy strategy) {
		return on(dataType, strategy, Importance.MANDATORY);
	}

	private RpcStrategyTypeDispatching on(Class<? extends RpcMessage.RpcMessageData> dataType,
	                                      RpcSingleSenderStrategy strategy, Importance importance) {
		checkNotNull(dataType);
		checkNotNull(strategy);
		checkNotNull(importance);
		dataTypeToSpecification.put(dataType, new DataTypeSpecifications(strategy, importance));
		return this;
	}

	public RpcStrategyTypeDispatching onDefault(RpcSingleSenderStrategy strategy) {
		checkState(defaultSendingStrategy == null, "Default Strategy is already set");
		defaultSendingStrategy = strategy;
		return this;
	}

	private static final class DataTypeSpecifications {
		private final RpcSingleSenderStrategy strategy;
		private final Importance importance;

		public DataTypeSpecifications(RpcSingleSenderStrategy strategy, Importance importance) {
			this.strategy = checkNotNull(strategy);
			this.importance = checkNotNull(importance);
		}

		public RpcSingleSenderStrategy getStrategy() {
			return strategy;
		}

		public Importance getImportance() {
			return importance;
		}
	}

	final static class RequestSenderTypeDispatcher implements RpcRequestSender {
		private static final RpcNoSenderAvailableException NO_SENDER_AVAILABLE_EXCEPTION
				= new RpcNoSenderAvailableException("No active senders available");

		private final HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender;
		private final RpcRequestSender defaultSender;

		public RequestSenderTypeDispatcher(HashMap<Class<? extends RpcMessage.RpcMessageData>, RpcRequestSender> dataTypeToSender,
		                                   RpcRequestSender defaultSender) {
			checkNotNull(dataTypeToSender);

			this.dataTypeToSender = dataTypeToSender;
			this.defaultSender = defaultSender;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
			RpcRequestSender specifiedSender = dataTypeToSender.get(request.getClass());
			RpcRequestSender sender = specifiedSender != null ? specifiedSender : defaultSender;
			if (sender != null) {
				sender.sendRequest(request, timeout, callback);
			} else {
				callback.onException(NO_SENDER_AVAILABLE_EXCEPTION);
			}
		}
	}
}
