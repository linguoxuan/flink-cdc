/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.entry;

import java.lang.reflect.Method;

/**
 * A minimal entry point JAR for Oceanus platform deployment.
 *
 * <p>This class is designed to be placed in {@code pipeline.jars} (which is loaded by
 * FlinkUserCodeClassLoader), while the actual Flink CDC dist JAR and connector JARs are placed in
 * {@code pipeline.classpaths} (which is loaded by the parent AppClassLoader).
 *
 * <p>By doing this, all CDC classes (flink-cdc-dist + connectors) reside in the same ClassLoader
 * (AppClassLoader), which resolves the ClassNotFoundException issue where connector classes cannot
 * find CDC common classes like EventDeserializer.
 *
 * <p>This class has ZERO dependencies on any CDC module. It uses pure Java reflection to invoke
 * {@code CliExecutor.main()} from the parent ClassLoader.
 */
public class CdcEntryPoint {

    private static final String CLI_EXECUTOR_CLASS = "org.apache.flink.cdc.cli.CliExecutor";

    public static void main(String[] args) throws Exception {
        // CliExecutor is in flink-cdc-dist.jar which should be in pipeline.classpaths
        // (AppClassLoader). Through parent delegation, we can load it from the child
        // FlinkUserCodeClassLoader.
        Class<?> cliExecutorClass;
        try {
            cliExecutorClass = Class.forName(CLI_EXECUTOR_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                    "Cannot find "
                            + CLI_EXECUTOR_CLASS
                            + ". "
                            + "Please make sure flink-cdc-dist JAR is placed in pipeline.classpaths.",
                    e);
        }

        Method mainMethod = cliExecutorClass.getMethod("main", String[].class);
        mainMethod.invoke(null, (Object) args);
    }
}
