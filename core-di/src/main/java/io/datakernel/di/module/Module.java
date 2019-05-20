package io.datakernel.di.module;

import io.datakernel.di.Binding;
import io.datakernel.di.Key;
import io.datakernel.di.Scope;

import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;

public interface Module {
	Map<Key<?>, Set<Binding<?>>> getBindings();

	Map<Scope, Map<Key<?>, Set<Binding<?>>>> getScopeBindings();

	Map<Key<?>, Function<Set<Binding<?>>, Binding<?>>> getConflictResolvers();
}
