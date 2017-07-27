/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.db.zk


import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreamstransactionserver.netty.server.streamService
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamRepository
import org.apache.curator.framework.CuratorFramework

final class ZookeeperStreamRepository(client: CuratorFramework,
                                      path: String)
  extends StreamRepository {

  private val streamCache =
    new ConcurrentHashMap[streamService.StreamKey, streamService.StreamValue]()
  private val streamNamePath = new StreamNamePath(client, s"$path/names")
  private val streamIdPath = new StreamIdPath(client, s"$path/ids")

  override def put(streamValue: streamService.StreamValue): streamService.StreamKey = {
    if (!streamNamePath.exists(streamValue.name)) {
      val streamRecord = streamIdPath.put(streamValue)
      streamNamePath.put(streamRecord)
      streamCache.put(streamRecord.key, streamRecord.stream)
      streamRecord.key
    } else
      streamService.StreamKey(-1)
  }

  override def exists(name: String): Boolean =
    streamNamePath.exists(name)


  override def delete(name: String): Boolean =
    streamNamePath.delete(name)


  override def get(name: String): Option[streamService.StreamRecord] =
    streamNamePath.get(name)


  override def get(streamKey: streamService.StreamKey): Option[streamService.StreamRecord] = {
    Option(streamCache.get(streamKey))
      .map(steamValue => streamService.StreamRecord(streamKey, steamValue))
      .orElse {
        val streamRecordOpt = streamIdPath.get(streamKey)
        streamRecordOpt.foreach(streamRecord =>
          streamCache.put(streamRecord.key, streamRecord.stream)
        )
        streamRecordOpt
      }
  }
}
