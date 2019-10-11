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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public final class SerializerGenArray implements SerializerGen, NullableOptimization {
	private final SerializerGen valueSerializer;
	private final int fixedSize;
	private final Class<?> type;
	private final boolean nullable;

	public SerializerGenArray(@NotNull SerializerGen serializer, int fixedSize, Class<?> type, boolean nullable) {
		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = nullable;
	}

	public SerializerGenArray(@NotNull SerializerGen serializer, int fixedSize, Class<?> type) {
		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = false;
	}

	public SerializerGenArray(SerializerGen serializer, Class<?> type) {
		this(serializer, -1, type);
	}

	public SerializerGenArray fixedSize(int fixedSize, Class<?> nameOfClass) {
		return new SerializerGenArray(valueSerializer, fixedSize, nameOfClass, nullable);
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return Object.class;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version,
			CompatibilityLevel compatibilityLevel) {
		Expression castedValue = cast(value, type);
		Expression length = fixedSize != -1 ? value(fixedSize) : length(castedValue);
		Expression writeLength = writeVarInt(byteArray, off, (!nullable ? length : inc(length)));
		Expression writeZeroLength = writeByte(byteArray, off, value((byte) 0));
		Expression writeByteArray = writeBytes(byteArray, off, castedValue);
		Expression writeCollection = loop(value(0), length,
				it -> valueSerializer.serialize(classLoader, byteArray, off, getArrayItem(castedValue, it), version, compatibilityLevel));

		if (!nullable) {
			return type.getComponentType() == Byte.TYPE ?
					sequence(writeLength, writeByteArray) :
					sequence(writeLength, writeCollection);
		} else {
			return type.getComponentType() == Byte.TYPE ?
					ifThenElse(isNull(value),
							writeZeroLength,
							sequence(writeLength, writeByteArray)
					) :
					ifThenElse(isNull(value),
							writeZeroLength,
							sequence(writeLength, writeCollection));
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return !nullable ?
				let(readVarInt(byteArray, off), len ->
						let(newArray(type, len), array ->
								sequence(
										type.getComponentType() == Byte.TYPE ?
												readBytes(byteArray, off, array) :
												loop(value(0), len,
														i -> setArrayItem(array, i,
																cast(valueSerializer.deserialize(classLoader, byteArray, off, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
										array))) :
				let(readVarInt(byteArray, off), len ->
						ifThenElse(cmpEq(len, value(0)),
								nullRef(type),
								let(newArray(type, dec(len)), array ->
										sequence(
												type.getComponentType() == Byte.TYPE ?
														readBytes(byteArray, off, array) :
														loop(value(0), dec(len),
																i -> setArrayItem(array, i,
																		cast(valueSerializer.deserialize(classLoader, byteArray, off, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
												array)
								)));
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenArray that = (SerializerGenArray) o;

		if (fixedSize != that.fixedSize) return false;
		if (nullable != that.nullable) return false;
		if (!Objects.equals(valueSerializer, that.valueSerializer)) return false;
		if (!Objects.equals(type, that.type)) return false;
		return true;

	}

	@Override
	public int hashCode() {
		int result = valueSerializer != null ? valueSerializer.hashCode() : 0;
		result = 31 * result + fixedSize;
		result = 31 * result + (type != null ? type.hashCode() : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}

	@Override
	public SerializerGen asNullable() {
		return new SerializerGenArray(valueSerializer, fixedSize, type, true);
	}
}
