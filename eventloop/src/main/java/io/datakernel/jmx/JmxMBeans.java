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

package io.datakernel.jmx;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ScheduledRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static io.datakernel.jmx.ReflectionUtils.*;
import static io.datakernel.util.Preconditions.*;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(JmxMBeans.class);

	// refreshing jmx
	public static final double DEFAULT_REFRESH_PERIOD_IN_SECONDS = 1.0;  // one second
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 500;
	private final int maxJmxRefreshesPerOneCycle;
	private final long specifiedRefreshPeriod;
	private final Map<Eventloop, List<Iterable<JmxRefreshable>>> eventloopToJmxRefreshables =
			new ConcurrentHashMap<>();

	private static final JmxReducer<?> DEFAULT_REDUCER = new JmxReducers.JmxReducerDistinct();

	private static final JmxMBeans INSTANCE_WITH_DEFAULT_REFRESH_PERIOD
			= new JmxMBeans(DEFAULT_REFRESH_PERIOD_IN_SECONDS, MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT);

	// region constructor and factory methods
	private JmxMBeans(double refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		this.specifiedRefreshPeriod = secondsToMillis(refreshPeriod);
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}

	public static JmxMBeans factory() {
		return INSTANCE_WITH_DEFAULT_REFRESH_PERIOD;
	}

	public static JmxMBeans factory(double refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		return new JmxMBeans(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}
	// endregion

	@Override
	public DynamicMBean createFor(List<?> monitorables, MBeanSetting setting, boolean enableRefresh) {
		checkNotNull(monitorables);
		checkArgument(monitorables.size() > 0);
		checkArgument(!listContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(monitorables));

		Object firstMBean = monitorables.get(0);
		Class<?> mbeanClass = firstMBean.getClass();

		boolean isRefreshEnabled = enableRefresh;

		List<MBeanWrapper> mbeanWrappers = new ArrayList<>(monitorables.size());
		if (ConcurrentJmxMBean.class.isAssignableFrom(mbeanClass)) {
			checkArgument(monitorables.size() == 1, "ConcurrentJmxMBeans cannot be used in pool. " +
					"Only EventloopJmxMBeans can be used in pool");
			isRefreshEnabled = false;
			mbeanWrappers.add(new ConcurrentJmxMBeanWrapper((ConcurrentJmxMBean) monitorables.get(0)));
		} else if (EventloopJmxMBean.class.isAssignableFrom(mbeanClass)) {
			for (Object monitorable : monitorables) {
				mbeanWrappers.add(new EventloopJmxMBeanWrapper((EventloopJmxMBean) monitorable));
			}
		} else {
			throw new IllegalArgumentException("MBeans should implement either ConcurrentJmxMBean " +
					"or EventloopJmxMBean interface");
		}

		AttributeNodeForPojo rootNode = (AttributeNodeForPojo)
				createAttributesTree(mbeanClass).rebuildOmittingNullPojos(monitorables);

		for (String included : setting.getIncludedOptionals()) {
			rootNode = (AttributeNodeForPojo) rootNode.rebuildWithVisible(included);
		}

		if (rootNode == null) {
			logger.warn("MBeans " + monitorables + " do not contain any attributes");
		}

		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, mbeanClass, isRefreshEnabled);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(mbeanClass);

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(
				mBeanInfo, mbeanWrappers, rootNode, opkeyToMethod, isRefreshEnabled
		);

		if (isRefreshEnabled && rootNode != null) {
			handleJmxRefreshables(mbeanWrappers, rootNode);
		}
		return mbean;
	}

	// region building tree of AttributeNodes
	private static List<AttributeNode> createNodesFor(Class<?> clazz, Class<?> mbeanClass,
	                                                  String[] includedOptionalAttrs, Method getter) {

		Set<String> includedOptionals = new HashSet<>(asList(includedOptionalAttrs));
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			check(descriptor.getGetter() != null, "@JmxAttribute \"%s\" does not have getter", descriptor.getName());

			String attrName;
			Method attrGetter = descriptor.getGetter();
			JmxAttribute attrAnnotation = attrGetter.getAnnotation(JmxAttribute.class);
			String attrAnnotationName = attrAnnotation.name();
			if (attrAnnotationName.equals(JmxAttribute.USE_GETTER_NAME)) {
				attrName = extractFieldNameFromGetter(attrGetter);
			} else {
				attrName = attrAnnotationName;
			}
			checkArgument(!attrName.contains("_"), "@JmxAttribute with name \"%s\" contains underscores", attrName);

			String attrDescription = null;
			if (!attrAnnotation.description().equals(JmxAttribute.NO_DESCRIPTION)) {
				attrDescription = attrAnnotation.description();
			}

			boolean included = attrAnnotation.optional() ? includedOptionals.contains(attrName) : true;
			includedOptionals.remove(attrName);
//			if (attrAnnotation.optional()) {
//				if (!includedOptionals.contains(attrName)) {
//					// do not include optional attributes by default
//					continue;
//				}
//
//				includedOptionals.remove(attrName);
//			}

			Type type = attrGetter.getGenericReturnType();
			Method attrSetter = descriptor.getSetter();
			AttributeNode attrNode = createAttributeNodeFor(attrName, attrDescription, type, included,
					attrAnnotation, attrGetter, attrSetter, mbeanClass);
			attrNodes.add(attrNode);
		}

		if (includedOptionals.size() > 0) {
			// in this case getter cannot be null
			throw new RuntimeException(format("Error in \"extraSubAttributes\" parameter in @JmxAnnotation" +
							" on %s.%s(). There is no field \"%s\" in %s.",
					getter.getDeclaringClass().getName(), getter.getName(),
					includedOptionals.iterator().next(), getter.getReturnType().getName()));
		}

		return attrNodes;
	}

	private static List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz) {
		Map<String, AttributeDescriptor> nameToAttr = new HashMap<>();
		for (Method method : clazz.getMethods()) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				if (isGetter(method)) {
					processGetter(nameToAttr, method);
				} else if (isSetter(method)) {
					processSetter(nameToAttr, method);
				} else {
					throw new RuntimeException(format("Method \"%s\" of class \"%s\" is annotated with @JmxAnnotation "
							+ "but is neither getter nor setter", method.getName(), method.getClass().getName())
					);
				}
			}
		}
		return new ArrayList<>(nameToAttr.values());
	}

	private static void processGetter(Map<String, AttributeDescriptor> nameToAttr, Method getter) {
		String name = extractFieldNameFromGetter(getter);
		Type attrType = getter.getReturnType();
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			check(previousDescriptor.getGetter() == null,
					"More that one getter with name" + getter.getName());
			check(previousDescriptor.getType().equals(attrType),
					"Getter with name \"%s\" has different type than appropriate setter", getter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, getter, previousDescriptor.getSetter()));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, null));
		}
	}

	private static void processSetter(Map<String, AttributeDescriptor> nameToAttr, Method setter) {
		checkArgument(isSimpleType(setter.getParameterTypes()[0]), "Setters are allowed only on SimpleType attributes."
				+ " But setter \"%s\" is not SimpleType setter", setter.getName());

		String name = extractFieldNameFromSetter(setter);
		Type attrType = setter.getParameterTypes()[0];
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			check(previousDescriptor.getSetter() == null,
					"More that one setter with name" + setter.getName());
			check(previousDescriptor.getType().equals(attrType),
					"Setter with name \"%s\" has different type than appropriate getter", setter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, previousDescriptor.getGetter(), setter));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, null, setter));
		}
	}

	@SuppressWarnings("unchecked")
	private static AttributeNode createAttributeNodeFor(String attrName, String attrDescription, Type attrType,
	                                                    boolean included,
	                                                    JmxAttribute attrAnnotation,
	                                                    Method getter, Method setter, Class<?> mbeanClass) {
		ValueFetcher defaultFetcher = getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
		if (attrType instanceof Class) {
			// 3 cases: simple-type, JmxRefreshableStats, POJO
			Class<?> returnClass = (Class<?>) attrType;
			if (isSimpleType(returnClass)) {
				JmxReducer<?> reducer;
				try {
					reducer = fetchReducerFrom(getter);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return new AttributeNodeForSimpleType(
						attrName, attrDescription, included, defaultFetcher, setter, returnClass, reducer
				);
			} else if (isThrowable(returnClass)) {
				return new AttributeNodeForThrowable(attrName, attrDescription, included, defaultFetcher);
			} else if (returnClass.isArray()) {
				Class<?> elementType = returnClass.getComponentType();
				checkNotNull(getter, "Arrays can be used only directly in POJO, JmxRefreshableStats or JmxMBeans");
				ValueFetcher fetcher = new ValueFetcherFromGetterArrayAdapter(getter);
				return createListAttributeNodeFor(attrName, attrDescription, included, fetcher, elementType, mbeanClass);
			} else if (isJmxStats(returnClass)) {
				// JmxRefreshableStats case

				checkJmxStatsAreValid(returnClass, mbeanClass, getter);

				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter);

				if (subNodes.size() == 0) {
					throw new IllegalArgumentException(format(
							"JmxRefreshableStats of type \"%s\" does not have JmxAttributes",
							returnClass.getName()));
				}

				if (isJmxRefreshableStats(returnClass)) {
					return new AttributeNodeForJmxRefreshableStats(attrName, attrDescription, included, defaultFetcher,
							(Class<? extends JmxRefreshableStats<?>>) returnClass,
							subNodes);
				} else {
					return new AttributeNodeForJmxStats(attrName, attrDescription, included, defaultFetcher,
							(Class<? extends JmxStats<?>>) returnClass,
							subNodes);
				}

			} else {
				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter);

				if (subNodes.size() == 0) {
					return new AttributeNodeForAnyOtherType(attrName, attrDescription, included, defaultFetcher);
				} else {
					// POJO case

					JmxReducer<?> reducer;
					try {
						reducer = fetchReducerFrom(getter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}

					if (reducer.getClass() == JmxAttribute.DEFAULT_REDUCER) {
						return new AttributeNodeForPojo(attrName, attrDescription, included, defaultFetcher, subNodes);
					} else {
						return new AttributeNodeForPojoWithReducer(attrName, attrDescription, included,
								defaultFetcher, reducer, subNodes);
					}
				}
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(
					attrName, attrDescription, (ParameterizedType) attrType, included, getter, mbeanClass
			);
		} else {
			throw new RuntimeException();
		}
	}

	private static JmxReducer<?> fetchReducerFrom(Method getter) throws IllegalAccessException, InstantiationException {
		if (getter == null) {
			return DEFAULT_REDUCER;
		} else {
			JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
			Class<? extends JmxReducer<?>> reducerClass = attrAnnotation.reducer();
			if (reducerClass == DEFAULT_REDUCER.getClass()) {
				return DEFAULT_REDUCER;
			} else {
				return reducerClass.newInstance();
			}
		}
	}

	private static void checkJmxStatsAreValid(Class<?> returnClass, Class<?> mbeanClass, Method getter) {
		if (!EventloopJmxMBean.class.isAssignableFrom(mbeanClass)) {
			throw new IllegalArgumentException("JmxRefreshableStats can be used only in classes that implements" +
					" EventloopJmxMBean");
		}

		if (returnClass.isInterface()) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (Modifier.isAbstract(returnClass.getModifiers())) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (!(classHasPublicNoArgConstructor(returnClass) || classHasStaticFactoryCreateMethod(returnClass))) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}
	}

	private static String createErrorMessageForInvalidJmxStatsAttribute(Method getter) {
		String msg = "Return type of JmxRefreshableStats attribute must be concrete class that implements" +
				" JmxRefreshableStats interface and contains public no-arg constructor " +
				"or static factory \"create\" method";
		if (getter != null) {
			msg += format(". Error at %s.%s()", getter.getDeclaringClass().getName(), getter.getName());
		}
		return msg;
	}

	private static AttributeNode createNodeForParametrizedType(String attrName, String attrDescription,
	                                                           ParameterizedType pType, boolean included,
	                                                           Method getter, Class<?> mbeanClass) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();
		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(
					attrName, attrDescription, included, fetcher, listElementType, mbeanClass);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, attrDescription, included, fetcher, valueType, mbeanClass);
		} else {
			throw new RuntimeException("There is no support for Generic classes other than List or Map");
		}
	}

	private static AttributeNodeForList createListAttributeNodeFor(String attrName, String attrDescription,
	                                                               boolean included,
	                                                               ValueFetcher fetcher,
	                                                               Type listElementType, Class<?> mbeanClass) {
		if (listElementType instanceof Class<?>) {
			Class<?> listElementClass = (Class<?>) listElementType;
			boolean isListOfJmxRefreshable = (JmxRefreshable.class.isAssignableFrom(listElementClass));
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, listElementType, true, null, null, null, mbeanClass),
					isListOfJmxRefreshable
			);
		} else if (listElementType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) listElementType).getRawType()).getSimpleName();
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createNodeForParametrizedType(
							typeName, attrDescription, (ParameterizedType) listElementType, true, null, mbeanClass
					),
					false
			);
		} else {
			throw new RuntimeException();
		}
	}

	private static AttributeNodeForMap createMapAttributeNodeFor(String attrName, String attrDescription,
	                                                             boolean included,
	                                                             ValueFetcher fetcher,
	                                                             Type valueType, Class<?> mbeanClass) {
		if (valueType instanceof Class<?>) {
			Class<?> valueClass = (Class<?>) valueType;
			boolean isMapOfJmxRefreshable = (JmxRefreshable.class.isAssignableFrom(valueClass));
			return new AttributeNodeForMap(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, valueType, true, null, null, null, mbeanClass),
					isMapOfJmxRefreshable
			);
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			return new AttributeNodeForMap(
					attrName,
					attrDescription,
					included,
					fetcher,
					createNodeForParametrizedType(
							typeName, attrDescription, (ParameterizedType) valueType, true, null, mbeanClass
					),
					false
			);
		} else {
			throw new RuntimeException();
		}
	}

	private static boolean isSimpleType(Class<?> clazz) {
		return isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz);
	}

	private static ValueFetcher createAppropriateFetcher(Method getter) {
		return getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
	}
	// endregion

	// region refreshing jmx
	private void handleJmxRefreshables(List<MBeanWrapper> mbeanWrappers, AttributeNodeForPojo rootNode) {
		for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
			Eventloop eventloop = mbeanWrapper.getEventloop();
			Iterable<JmxRefreshable> currentRefreshables = rootNode.getAllRefreshables(mbeanWrapper.getMBean());
			if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
				eventloopToJmxRefreshables.put(
						eventloop,
						asList(currentRefreshables)
				);
				eventloop.post(createRefreshTask(eventloop, null, 0, 0));
			} else {
				List<Iterable<JmxRefreshable>> previousRefreshables = eventloopToJmxRefreshables.get(eventloop);
				List<Iterable<JmxRefreshable>> allRefreshables = new ArrayList<>(previousRefreshables);
				allRefreshables.add(currentRefreshables);
				eventloopToJmxRefreshables.put(eventloop, allRefreshables);
			}
		}
	}

	private ScheduledRunnable createRefreshTask(final Eventloop eventloop,
	                                            final Iterator<JmxRefreshable> previousIterator,
	                                            final int previousIteratorRefreshesCount,
	                                            final int supposedJmxRefreshablesCount) {
		return new ScheduledRunnable() {
			@Override
			public void run() {
				long currentTime = eventloop.currentTimeMillis();

				Iterator<JmxRefreshable> jmxRefreshableIterator;
				if (previousIterator == null) {
					List<Iterable<JmxRefreshable>> listOfIterables = eventloopToJmxRefreshables.get(eventloop);
					jmxRefreshableIterator = Utils.concat(listOfIterables).iterator();
				} else {
					jmxRefreshableIterator = previousIterator;
				}

				int currentRefreshesCount = 0;
				while (jmxRefreshableIterator.hasNext()) {
					if (currentRefreshesCount > maxJmxRefreshesPerOneCycle) {
						long period = computeEffectiveRefreshPeriod(supposedJmxRefreshablesCount);
						eventloop.scheduleBackground(
								currentTime + period,
								createRefreshTask(
										eventloop,
										jmxRefreshableIterator,
										previousIteratorRefreshesCount + currentRefreshesCount,
										supposedJmxRefreshablesCount
								)
						);
						return;
					}
					jmxRefreshableIterator.next().refresh(currentTime);
					currentRefreshesCount++;
				}

				int freshJmxRefreshablesCount = previousIteratorRefreshesCount + currentRefreshesCount;
				long period = computeEffectiveRefreshPeriod(freshJmxRefreshablesCount);
				eventloop.scheduleBackground(
						currentTime + period,
						createRefreshTask(
								eventloop,
								null,
								0,
								freshJmxRefreshablesCount
						)
				);
			}
		};
	}

	private long computeEffectiveRefreshPeriod(int actualRefreshes) {
		if (actualRefreshes == 0) {
			return specifiedRefreshPeriod;
		}
		double ratio = ceil(actualRefreshes / (double) maxJmxRefreshesPerOneCycle);
		return (long) (specifiedRefreshPeriod / ratio);
	}

	private static AttributeNodeForPojo createAttributesTree(Class<?> clazz) {
		List<AttributeNode> subNodes = createNodesFor(clazz, clazz, new String[0], null);
		AttributeNodeForPojo root = new AttributeNodeForPojo("", null, true, new ValueFetcherDirect(), subNodes);
		return root;
	}
	// endregion

	// region creating jmx metadata - MBeanInfo
	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode,
	                                         Class<?> monitorableClass,
	                                         boolean enableRefresh) {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = rootNode != null ?
				fetchAttributesInfo(rootNode, enableRefresh) :
				new MBeanAttributeInfo[0];
		MBeanOperationInfo[] operations = fetchOperationsInfo(monitorableClass, enableRefresh);
		return new MBeanInfo(
				monitorableName,
				monitorableDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	private static MBeanAttributeInfo[] fetchAttributesInfo(AttributeNodeForPojo rootNode, boolean refreshEnabled) {
		Map<String, OpenType<?>> nameToType = rootNode.getVisibleFlattenedOpenTypes();
		Map<String, Map<String, String>> nameToDescriptions = rootNode.getDescriptions();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : nameToType.keySet()) {
			String description = createDescription(attrName, nameToDescriptions.get(attrName));
			OpenType<?> attrType = nameToType.get(attrName);
			boolean writable = rootNode.isSettable(attrName);
			boolean isIs = attrType.equals(SimpleType.BOOLEAN);
			// TODO(vmykhalko): refactor
			attrsInfo.add(new MBeanAttributeInfo(
					removeTrailingUnderscore(attrName), attrType.getClassName(), description, true, writable, isIs));
		}

		return attrsInfo.toArray(new MBeanAttributeInfo[attrsInfo.size()]);
	}

	private static String removeTrailingUnderscore(String str) {
		return str.charAt(str.length() - 1) == '_' ? str.substring(0, str.length() - 1) : str;
	}

	private static String createDescription(String name, Map<String, String> groupDescriptions) {
		// TODO(vmykhalko): remove debug code
		if (groupDescriptions == null) {
			System.out.println("wtf");
		}

		if (groupDescriptions.isEmpty()) {
			return name;
		}

		if (!name.contains("_")) {
			assert groupDescriptions.size() == 1;
			return groupDescriptions.values().iterator().next();
		}

		String descriptionTemplate = "\"%s\": %s";
		String separator = "  |  ";
		StringBuilder totalDescription = new StringBuilder("");
		for (String groupName : groupDescriptions.keySet()) {
			String groupDescription = groupDescriptions.get(groupName);
			totalDescription.append(String.format(descriptionTemplate, groupName, groupDescription));
			totalDescription.append(separator);
		}
		totalDescription.delete(totalDescription.length() - separator.length(), totalDescription.length());
		return totalDescription.toString();
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> monitorableClass, boolean enableRefresh) {
		List<MBeanOperationInfo> operations = new ArrayList<>();
		Method[] methods = monitorableClass.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}
				String opDescription = annotation.description();
				Class<?> returnType = method.getReturnType();
				List<MBeanParameterInfo> params = new ArrayList<>();
				Class<?>[] paramTypes = method.getParameterTypes();
				Annotation[][] paramAnnotations = method.getParameterAnnotations();

				assert paramAnnotations.length == paramTypes.length;

				for (int i = 0; i < paramTypes.length; i++) {
					String paramName = String.format("arg%d", i);
					Class<?> paramType = paramTypes[i];
					JmxParameter nameAnnotation = findJmxNamedParameterAnnotation(paramAnnotations[i]);
					if (nameAnnotation != null) {
						paramName = nameAnnotation.value();
					}
					MBeanParameterInfo paramInfo = new MBeanParameterInfo(paramName, paramType.getName(), "");
					params.add(paramInfo);
				}
				MBeanParameterInfo[] paramsArray = params.toArray(new MBeanParameterInfo[params.size()]);
				MBeanOperationInfo operationInfo = new MBeanOperationInfo(
						opName, opDescription, paramsArray, returnType.getName(), MBeanOperationInfo.ACTION);
				operations.add(operationInfo);
			}
		}

		return operations.toArray(new MBeanOperationInfo[operations.size()]);
	}

	private static JmxParameter findJmxNamedParameterAnnotation(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(JmxParameter.class)) {
				return (JmxParameter) annotation;
			}
		}
		return null;
	}
	// endregion

	// region jmx operations fetching
	private static Map<OperationKey, Method> fetchOpkeyToMethod(Class<?> mbeanClass) {
		Map<OperationKey, Method> opkeyToMethod = new HashMap<>();
		Method[] methods = mbeanClass.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}
				Class<?>[] paramTypes = method.getParameterTypes();
				Annotation[][] paramAnnotations = method.getParameterAnnotations();

				assert paramAnnotations.length == paramTypes.length;

				String[] paramTypesNames = new String[paramTypes.length];
				for (int i = 0; i < paramTypes.length; i++) {
					paramTypesNames[i] = paramTypes[i].getName();
				}
				opkeyToMethod.put(new OperationKey(opName, paramTypesNames), method);
			}
		}
		return opkeyToMethod;
	}
	// endregion

	// region etc
	private static int secondsToMillis(double seconds) {
		return (int) (seconds * 1000);
	}

	private static <T> boolean listContainsNullValues(List<T> list) {
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) == null) {
				return true;
			}
		}
		return false;
	}

	private static boolean allObjectsAreOfSameType(List<?> objects) {
		for (int i = 0; i < objects.size() - 1; i++) {
			Object current = objects.get(i);
			Object next = objects.get(i + 1);
			if (!current.getClass().equals(next.getClass())) {
				return false;
			}
		}
		return true;
	}
	// endregion

	// region helper classes
	private static final class AttributeDescriptor {
		private final String name;
		private final Type type;
		private final Method getter;
		private final Method setter;

		public AttributeDescriptor(String name, Type type, Method getter, Method setter) {
			this.name = name;
			this.type = type;
			this.getter = getter;
			this.setter = setter;
		}

		public String getName() {
			return name;
		}

		public Type getType() {
			return type;
		}

		public Method getGetter() {
			return getter;
		}

		public Method getSetter() {
			return setter;
		}
	}

	private static final class OperationKey {
		private final String name;
		private final String[] argTypes;

		public OperationKey(String name, String[] argTypes) {
			checkNotNull(name);
			checkNotNull(argTypes);

			this.name = name;
			this.argTypes = argTypes;
		}

		public String getName() {
			return name;
		}

		public String[] getArgTypes() {
			return argTypes;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof OperationKey)) return false;

			OperationKey that = (OperationKey) o;

			if (!name.equals(that.name)) return false;

			return Arrays.equals(argTypes, that.argTypes);

		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + Arrays.hashCode(argTypes);
			return result;
		}
	}

	private static final class DynamicMBeanAggregator implements DynamicMBean {
		private final MBeanInfo mBeanInfo;
		private final List<? extends MBeanWrapper> mbeanWrappers;
		private final List<?> mbeans;
		private final AttributeNodeForPojo rootNode;
		private final Map<OperationKey, Method> opkeyToMethod;

		// TODO(vmykhalko): refactor
		private final Set<String> namesWithRemovedTrailingUnderscore = new HashSet<>();

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<? extends MBeanWrapper> mbeanWrappers,
		                              AttributeNodeForPojo rootNode, Map<OperationKey, Method> opkeyToMethod,
		                              boolean refreshEnabled) {
			this.mBeanInfo = mBeanInfo;
			this.mbeanWrappers = mbeanWrappers;

			List<Object> extractedMBeans = new ArrayList<>(mbeanWrappers.size());
			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				extractedMBeans.add(mbeanWrapper.getMBean());
			}
			this.mbeans = extractedMBeans;

			this.rootNode = rootNode;
			this.opkeyToMethod = opkeyToMethod;

			// TODO(vmykhalko): refactor
			if (rootNode != null) {
				for (String name : rootNode.getVisibleFlattenedOpenTypes().keySet()) {
					if (name.charAt(name.length() - 1) == '_') {
						namesWithRemovedTrailingUnderscore.add(removeTrailingUnderscore(name));
					}
				}
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object getAttribute(String attribute)
				throws AttributeNotFoundException, MBeanException, ReflectionException {
			// TODO(vmykhalko): refactor
			String attrName = namesWithRemovedTrailingUnderscore.contains(attribute) ?
					attribute + "_" :
					attribute;
			return rootNode.aggregateAttribute(attrName, mbeans);
		}

		@Override
		public void setAttribute(final Attribute attribute)
				throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
			// TODO(vmykhalko): refactor
			final String attrName = namesWithRemovedTrailingUnderscore.contains(attribute.getName()) ?
					attribute.getName() + "_" :
					attribute.getName();
			final Object attrValue = attribute.getValue();

			final CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				final Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute((new Runnable() {
					@Override
					public void run() {
						try {
							rootNode.setAttribute(attrName, attrValue, asList(mbean));
							latch.countDown();
						} catch (Exception e) {
							exceptionReference.set(e);
							latch.countDown();
						}
					}
				}));
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new MBeanException(e);
			}

			Exception exception = exceptionReference.get();
			if (exception != null) {
				Exception actualException = exception;
				if (exception instanceof SetterException) {
					SetterException setterException = (SetterException) exception;
					actualException = setterException.getCausedException();
				}
				propagateException(actualException);
			}
		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			checkArgument(attributes != null);

			AttributeList attrList = new AttributeList();
			for (String attrName : attributes) {
				try {
					attrList.add(new Attribute(attrName, getAttribute(attrName)));
				} catch (AttributeNotFoundException | MBeanException | ReflectionException e) {
					logger.error("Cannot get attribute: " + attrName, e);
				}
			}
			return attrList;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			AttributeList resultList = new AttributeList();
			for (int i = 0; i < attributes.size(); i++) {
				Attribute attribute = (Attribute) attributes.get(i);
				try {
					setAttribute(attribute);
					resultList.add(new Attribute(attribute.getName(), attribute.getValue()));
				} catch (AttributeNotFoundException | InvalidAttributeValueException
						| MBeanException | ReflectionException e) {
					logger.error("Cannot set attribute: " + attribute.getName(), e);
				}
			}
			return resultList;
		}

		@Override
		public Object invoke(final String actionName, final Object[] params, final String[] signature)
				throws MBeanException, ReflectionException {

			String[] argTypes = signature != null ? signature : new String[0];
			final Object[] args = params != null ? params : new Object[0];
			OperationKey opkey = new OperationKey(actionName, argTypes);
			final Method opMethod = opkeyToMethod.get(opkey);
			if (opMethod == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = "There is no operation \"" + operationName + "\"";
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			final CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

			final AtomicReference lastValue = new AtomicReference();
			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				final Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute((new Runnable() {
					@Override
					public void run() {
						try {
							Object result = opMethod.invoke(mbean, args);
							lastValue.set(result);
							latch.countDown();
						} catch (Exception e) {
							exceptionReference.set(e);
							latch.countDown();
						}
					}
				}));
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new MBeanException(e);
			}

			Exception exception = exceptionReference.get();
			if (exception != null) {
				propagateException(exception);
			}

			// We don't know how to aggregate return values if there are several mbeans
			return mbeanWrappers.size() == 1 ? lastValue.get() : null;
		}

		private void propagateException(Exception exception) throws MBeanException {
			if (exception instanceof InvocationTargetException) {
				Throwable targetException = ((InvocationTargetException) exception).getTargetException();

				if (targetException instanceof Exception) {
					throw new MBeanException((Exception) targetException);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown during method invocation",
									targetException.getClass().getName(), targetException.getMessage())
							)
					);
				}

			} else {
				throw new MBeanException(exception);
			}
		}

		private static String prettyOperationName(String name, String[] argTypes) {
			String operationName = name + "(";
			if (argTypes.length > 0) {
				for (int i = 0; i < argTypes.length - 1; i++) {
					operationName += argTypes[i] + ", ";
				}
				operationName += argTypes[argTypes.length - 1];
			}
			operationName += ")";
			return operationName;
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}
	}

	private interface MBeanWrapper {
		void execute(Runnable command);

		Object getMBean();

		Eventloop getEventloop();
	}

	private static final class ConcurrentJmxMBeanWrapper implements MBeanWrapper {
		private final ConcurrentJmxMBean mbean;

		public ConcurrentJmxMBeanWrapper(ConcurrentJmxMBean mbean) {
			this.mbean = mbean;
		}

		@Override
		public void execute(Runnable command) {
			command.run();
		}

		@Override
		public Object getMBean() {
			return mbean;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	private static final class EventloopJmxMBeanWrapper implements MBeanWrapper {
		private final EventloopJmxMBean mbean;

		public EventloopJmxMBeanWrapper(EventloopJmxMBean mbean) {
			this.mbean = mbean;
		}

		@Override
		public void execute(Runnable command) {
			mbean.getEventloop().execute(command);
		}

		@Override
		public Object getMBean() {
			return mbean;
		}

		@Override
		public Eventloop getEventloop() {
			return mbean.getEventloop();
		}
	}
	// endregion
}
