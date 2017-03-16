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

package io.datakernel.rpc.server;

import io.datakernel.async.ResultCallback;

/**
 * Implementations of this interface specifies the behavior according to
 * business logic and passes result to callback.
 * <p>
 * An example of concrete {@code RpcRequestHandler} can be found in
 * {@link RpcServer} documentation.
 *
 * @param <I>	class of request
 * @param <O>	class of response
 */
public interface RpcRequestHandler<I, O> {
	void run(I request, ResultCallback<O> callback);
}
