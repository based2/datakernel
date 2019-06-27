package io.datakernel.di.core;

import io.datakernel.di.annotation.EagerSingleton;
import io.datakernel.di.module.DefaultModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.module.Multibinder;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.di.core.BindingGenerator.REFUSING;
import static io.datakernel.di.core.BindingGenerator.combinedGenerator;
import static io.datakernel.di.core.BindingTransformer.IDENTITY;
import static io.datakernel.di.core.BindingTransformer.combinedTransformer;
import static io.datakernel.di.module.Multibinder.ERROR_ON_DUPLICATE;
import static io.datakernel.di.module.Multibinder.combinedMultibinder;
import static io.datakernel.di.util.Utils.*;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

@SuppressWarnings("unused")
public class Injector {
	@Nullable
	private final Injector parent;
	@Nullable
	private final Scope scope;

	private final Trie<Scope, Map<Key<?>, Binding<?>>> bindings;
	private final Map<Key<?>, Binding<?>> localBindings;
	private final Map<Key<?>, Object> instances;

	protected static final class SynchronizedInjector extends Injector {
		protected SynchronizedInjector(@Nullable Injector parent, @Nullable Scope scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
			super(parent, scope, bindings, instances);
		}

		@Override
		synchronized public <T> @NotNull T getInstance(@NotNull Key<T> key) {
			return super.getInstance(key);
		}

		@Override
		synchronized public <T> @Nullable T getInstanceOrNull(@NotNull Key<T> key) {
			return super.getInstanceOrNull(key);
		}

		@Override
		synchronized public <T> @NotNull T createInstance(@NotNull Key<T> key) {
			return super.createInstance(key);
		}

		@Override
		synchronized public <T> @Nullable T createInstanceOrNull(@NotNull Key<T> key) {
			return super.createInstanceOrNull(key);
		}

		@Override
		synchronized public <T> @Nullable T peekInstance(@NotNull Key<T> key) {
			return super.peekInstance(key);
		}

		@Override
		synchronized public Map<Key<?>, Object> peekInstances() {
			return super.peekInstances();
		}

		@Override
		synchronized public boolean hasInstance(@NotNull Key<?> type) {
			return super.hasInstance(type);
		}

		@Override
		public boolean isThreadSafe() {
			return true;
		}
	}

	private static final Object[] NO_OBJECTS = new Object[0];
	private static final Object NO_KEY = new Object();

	private Injector(@Nullable Injector parent, @Nullable Scope scope, Trie<Scope, Map<Key<?>, Binding<?>>> bindings, Map<Key<?>, Object> instances) {
		this.parent = parent;
		this.scope = scope;
		this.bindings = bindings;
		this.localBindings = bindings.get();
		this.instances = instances;
	}

	public static Injector of(Module... modules) {
		return compile(Modules.combine(Modules.combine(modules), new DefaultModule()));
	}

	public static Injector of(@NotNull Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return compile(null, new HashMap<>(), true, null, bindings.map(Utils::toMultimap), ERROR_ON_DUPLICATE, IDENTITY, REFUSING);
	}

	public static Injector compile(Module module) {
		return compile(null, new HashMap<>(), true,
				null, module.getBindings(),
				combinedMultibinder(module.getMultibinders()),
				combinedTransformer(module.getBindingTransformers()),
				combinedGenerator(module.getBindingGenerators()));
	}

	public static Injector compile(@Nullable Injector parent, Map<Key<?>, Object> instances, boolean threadsafe,
			@Nullable Scope scope,
			@NotNull Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindingsMultimap,
			@NotNull Multibinder<?> multibinder,
			@NotNull BindingTransformer<?> transformer,
			@NotNull BindingGenerator<?> generator) {

		Trie<Scope, Map<Key<?>, Binding<?>>> bindings = resolve(bindingsMultimap, multibinder);

		Injector injector = threadsafe ?
				new SynchronizedInjector(parent, scope, bindings, instances) :
				new Injector(parent, scope, bindings, instances);

		// well, can't do anything better than that
		bindings.get().put(Key.of(Injector.class), Binding.toInstance(injector));

		BindingGraph.completeBindingGraph(bindings, transformer, generator);

		Map<Key<?>, Set<Map.Entry<Key<?>, Binding<?>>>> unsatisfied = BindingGraph.getUnsatisfiedDependencies(bindings);
		if (!unsatisfied.isEmpty()) {
			throw new DIException(unsatisfied.entrySet().stream()
					.map(entry -> entry.getValue().stream()
							.map(binding -> binding.getKey().getDisplayString() + " " + Utils.getLocation(binding.getValue()))
							.collect(joining("\n\t\t     and ", "\tkey " + entry.getKey().getDisplayString() + ":\n\t\trequired to make ", "")))
					.collect(joining("\n", "Unsatisfied dependencies detected:\n", "\n")));
		}

		Set<Key<?>[]> cycles = BindingGraph.getCyclicDependencies(bindings);
		if (!cycles.isEmpty()) {
			throw new DIException(cycles.stream()
					.map(cycle -> {
						Name firstName = cycle[0].getName();
						int nameOffset = firstName != null ? firstName.getDisplayString().length() + 1 : 0;
						int offset = nameOffset + (cycle[0].getDisplayString().length() - nameOffset) / 2;
						String cycleString = Arrays.stream(cycle).map(Key::getDisplayString).collect(joining(" -> ", "\t", ""));
						String indent = new String(new char[offset]).replace('\0', ' ');
						String line = new String(new char[cycleString.length() - offset]).replace('\0', '-');
						return cycleString + " -,\n\t" + indent + "^" + line + "'";
					})
					.collect(joining("\n\n", "Cyclic dependencies detected:\n\n", "\n")));
		}

		return injector;
	}

	@NotNull
	public <T> T getInstance(@NotNull Class<T> type) {
		return getInstance(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@NotNull
	public <T> T getInstance(@NotNull Key<T> key) {
		T instance = (T) instances.getOrDefault(key, NO_KEY);
		if (instance != NO_KEY) {
			return instance;
		}
		instance = doCreateInstanceOrNull(key);
		instances.put(key, instance);
		if (instance == null) {
			throw cannotConstruct(key, localBindings.get(key));
		}
		return instance;
	}

	public <T> T getInstanceOr(@NotNull Class<T> type, T defaultValue) {
		T instanceOrNull = getInstanceOrNull(type);
		return instanceOrNull != null ? instanceOrNull : defaultValue;
	}

	public <T> T getInstanceOr(@NotNull Key<T> key, T defaultValue) {
		T instanceOrNull = getInstanceOrNull(key);
		return instanceOrNull != null ? instanceOrNull : defaultValue;
	}

	@Nullable
	public <T> T getInstanceOrNull(@NotNull Class<T> type) {
		return getInstanceOrNull(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> T getInstanceOrNull(@NotNull Key<T> key) {
		T instance = (T) instances.getOrDefault(key, NO_KEY);
		if (instance != NO_KEY) {
			return instance;
		}
		instance = doCreateInstanceOrNull(key);
		instances.put(key, instance);
		return instance;
	}

	@NotNull
	public <T> T createInstance(@NotNull Class<T> type) {
		return createInstance(Key.of(type));
	}

	@NotNull
	public <T> T createInstance(@NotNull Key<T> key) {
		T instance = doCreateInstanceOrNull(key);
		if (instance == null) {
			throw cannotConstruct(key, localBindings.get(key));
		}
		return instance;
	}

	@Nullable
	public <T> T createInstanceOrNull(@NotNull Class<T> type) {
		return createInstanceOrNull(Key.of(type));
	}

	@Nullable
	public <T> T createInstanceOrNull(@NotNull Key<T> key) {
		return doCreateInstanceOrNull(key);
	}

	@SuppressWarnings("unchecked")
	@Nullable
	private <T> T doCreateInstanceOrNull(@NotNull Key<T> key) {
		Binding<?> binding = localBindings.get(key);
		if (binding != null) {
			Dependency[] dependencies = binding.getDependencies();
			if (dependencies.length == 0) {
				return (T) binding.getFactory().create(NO_OBJECTS);
			}
			Object[] dependencyInstances = new Object[dependencies.length];
			for (int i = 0; i < dependencies.length; i++) {
				Dependency dependency = dependencies[i];
				Key<?> dependencyKey = dependency.getKey();
				Object dependencyInstance = instances.get(dependencyKey);
				if (dependencyInstance == null) {
					dependencyInstance = doCreateInstanceOrNull(dependencyKey);
					if (dependencyInstance != null) {
						instances.put(dependencyKey, dependencyInstance);
					}
				}
				if (dependencyInstance == null && dependency.isRequired()) {
					throw cannotConstruct(dependencyKey, localBindings.get(dependencyKey));
				}
				dependencyInstances[i] = dependencyInstance;
			}
			return (T) binding.getFactory().create(dependencyInstances);
		}
		if (parent != null) {
			return parent.getInstanceOrNull(key);
		}
		return null;
	}

	private static DIException cannotConstruct(Key<?> key, @Nullable Binding<?> binding) {
		return new DIException((binding != null ? "Binding refused to" : "No binding to") + " construct an instance for key " +
				key.getDisplayString() + (binding != null && binding.getLocation() != null ? ("\n\t at" + binding.getLocation()) : ""));
	}

	@Nullable
	public <T> T peekInstance(@NotNull Class<T> type) {
		return peekInstance(Key.of(type));
	}

	@Nullable
	@SuppressWarnings("unchecked")
	public <T> T peekInstance(@NotNull Key<T> key) {
		return (T) instances.get(key);
	}

	public boolean hasInstance(@NotNull Class<?> type) {
		return hasInstance(Key.of(type));
	}

	public boolean hasInstance(@NotNull Key<?> type) {
		return instances.get(type) != null;
	}

	public Map<Key<?>, Object> peekInstances() {
		Map<Key<?>, Object> result = new HashMap<>();
		for (Map.Entry<Key<?>, Object> entry : instances.entrySet()) {
			Key<?> key = entry.getKey();
			Object value = entry.getValue();
			if (hasBinding(key) && value != null) {
				result.put(key, value);
			}
		}
		return result;
	}

	public Set<Key<?>> createEagerSingletons() {
		Set<Key<?>> eagerSingletons = getInstanceOr(new Key<Set<Key<?>>>(EagerSingleton.class) {}, emptySet());
		eagerSingletons.forEach(this::getInstance);
		return eagerSingletons;
	}

	@SuppressWarnings("unchecked")
	public Set<Key<?>> postInjectInstances() {
		Set<InstanceInjector<?>> postInjectors = getInstanceOr(new Key<Set<InstanceInjector<?>>>() {}, emptySet());
		for (InstanceInjector<?> instanceInjector : postInjectors) {
			Object instance = getInstance(instanceInjector.key());
			((InstanceInjector<Object>) instanceInjector).injectInto(instance);
		}
		return postInjectors.stream().map(InstanceInjector::key).collect(toSet());
	}

	@Nullable
	public Injector getParent() {
		return parent;
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	public Scope[] getScopes() {
		if (scope == null) {
			return Scope.UNSCOPED;
		}
		if (parent == null) {
			return new Scope[]{scope};
		}
		return next(parent.getScopes(), scope);
	}

	public Trie<Scope, Map<Key<?>, Binding<?>>> getBindings() {
		return bindings;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Class<T> type) {
		return (Binding<T>) localBindings.get(Key.of(type));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public <T> Binding<T> getBinding(Key<T> key) {
		return (Binding<T>) localBindings.get(key);
	}

	public boolean hasBinding(Class<?> type) {
		return hasBinding(Key.of(type));
	}

	public boolean hasBinding(Key<?> key) {
		return localBindings.containsKey(key);
	}

	public Injector enterScope(@NotNull Scope scope) {
		return enterScope(scope, new HashMap<>(), isThreadSafe());
	}

	public Injector enterScope(@NotNull Scope scope, @NotNull Map<Key<?>, Object> instances, boolean threadsafe) {
		Trie<Scope, Map<Key<?>, Binding<?>>> subBindings = bindings.get(scope);
		if (subBindings == null) {
			throw new DIException("Tried to enter scope " + getScopeDisplayString(next(getScopes(), scope)) + " that was not represented by any binding");
		}
		return threadsafe ?
				new SynchronizedInjector(this, scope, subBindings, instances) :
				new Injector(this, scope, subBindings, instances);
	}

	public boolean isThreadSafe() {
		return false;
	}
}
