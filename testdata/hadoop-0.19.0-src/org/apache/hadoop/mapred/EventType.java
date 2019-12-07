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
/**
 * {@link JobStatusChangeEvent} tracks the change in job's status. Job's 
 * status can change w.r.t 
 *   - run state i.e PREP, RUNNING, FAILED, KILLED, SUCCEEDED
 *   - start time
 *   - priority
 * Note that job times can change as the job can get restarted.
 */
package org.apache.hadoop.mapred;

static enum EventType {RUN_STATE_CHANGED, START_TIME_CHANGED, PRIORITY_CHANGED}