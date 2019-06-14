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

package io.datakernel.examples;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.di.module.Modules.combine;

public class UIKernelWebAppLauncher extends Launcher {
	static {
		LoggerConfigurer.enableSLF4Jbridge();
	}
	@Inject
	AsyncHttpServer server;

	@Override
	protected Module getModule() {
		return combine(ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.ofClassPathProperties("configs.properties")),
				new UIKernelWebAppModule());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		UIKernelWebAppLauncher launcher = new UIKernelWebAppLauncher();
		launcher.launch(args);
	}
}
