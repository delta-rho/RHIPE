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

public class RHTypes {
    // Commands to client

    public static final int EVAL_SETUP_MAP = -1;
    public static final int EVAL_CLEANUP_MAP = -2;

    public static final int EVAL_SETUP_REDUCE   = -1; 
    public static final int EVAL_REDUCE_PREKEY  = -2;
    public static final int EVAL_REDUCE_POSTKEY = -3;
    public static final int EVAL_REDUCE_THEKEY  = -4;
    public static final int EVAL_CLEANUP_REDUCE = -5;
    public static final int EVAL_FLUSH = -10;

    // Reading on STDERR

    public static final byte ERROR_MSG = (byte)0x00;
    public static final byte PRINT_MSG = (byte)0x01;
    public static final byte SET_STATUS = (byte)0x02;
    public static final byte SET_COUNTER = (byte)0x03;


}