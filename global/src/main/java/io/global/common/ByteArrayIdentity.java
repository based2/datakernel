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

package io.global.common;

/**
 * CONTRACT:
 * <p>
 * Since there is no way in Java as of now to specify static members
 * of classes which implement thre interface, I am saying it here in
 * the doc:
 * <p>
 * Classes which implement this interface must also define a static
 * factory method <code>ofBytes</code> which creates a new object
 * from single byte array, that was aquired using the
 * {@link ByteArrayIdentity#toBytes toBytes()} method.
 */
public interface ByteArrayIdentity {
	byte[] toBytes();
}
