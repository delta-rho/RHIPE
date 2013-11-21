/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This is a class used to get the current environment
 * on the host machines running the map/reduce. This class
 * assumes that setting the environment in streaming is
 * allowed on windows/ix/linuz/freebsd/sunos/solaris/hp-ux
 */
public class Environment extends Properties {

    public Environment() throws IOException {
        // Extend this code to fit all operating
        // environments that you expect to run in
        // http://lopica.sourceforge.net/os.html
        String command = null;
        final String os = System.getProperty("os.name").toLowerCase();

        if (os.contains("windows")) {
            command = "cmd /C set";
        }
        else if (os.contains("ix") || os.contains("linux") || os.contains("freebsd") ||
                os.contains("sunos") || os.contains("solaris") || os.contains("hp-ux")) {
            command = "env";
        }
        else if (os.startsWith("mac os x") || os.startsWith("darwin")) {
            command = "env";
        }

        if (command == null) {
            throw new RuntimeException("Operating system " + os + " not supported by this class");
        }

        // Read the environment variables

        final Process pid = Runtime.getRuntime().exec(command);
        final BufferedReader in = new BufferedReader(new InputStreamReader(pid.getInputStream()));
        while (true) {
            final String line = in.readLine();
            if (line == null) {
                break;
            }
            final int p = line.indexOf("=");
            if (p != -1) {
                final String name = line.substring(0, p);
                final String value = line.substring(p + 1);
                setProperty(name, value);
            }
        }
        in.close();
        try {
            pid.waitFor();
        }
        catch (InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }

    // to be used with Runtime.exec(String[] cmdarray, String[] envp)
    public String[] toArray() {
        final String[] arr = new String[super.size()];
        final Enumeration it = super.keys();
        int i = -1;
        while (it.hasMoreElements()) {
            final String key = (String) it.nextElement();
            final String val = (String) get(key);
            i++;
            arr[i] = key + "=" + val;
        }
        return arr;
    }

    public Map<String, String> toMap() {
        final Map<String, String> map = new HashMap<String, String>();
        final Enumeration<Object> it = super.keys();
        while (it.hasMoreElements()) {
            final String key = (String) it.nextElement();
            final String val = (String) get(key);
            map.put(key, val);
        }
        return map;
    }

    public String getHost() {
        String host = getProperty("HOST");
        if (host == null) {
            // HOST isn't always in the environment
            try {
                host = InetAddress.getLocalHost().getHostName();
            }
            catch (IOException io) {
                io.printStackTrace();
            }
        }
        return host;
    }

}
