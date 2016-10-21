/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.predictionio.data.storage.druid


import org.apache.predictionio.annotation.DeveloperApi
import org.apache.predictionio.data.storage.{Event, PEvents}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  */
class DruidPEvents extends PEvents {
  /** :: DeveloperApi ::
    * Read from database and return the events. The deprecation here is intended
    * to engine developers only.
    *
    * @param appId            return events of this app ID
    * @param channelId        return events of this channel ID (default channel if it's None)
    * @param startTime        return events with eventTime >= startTime
    * @param untilTime        return events with eventTime < untilTime
    * @param entityType       return events of this entityType
    * @param entityId         return events of this entityId
    * @param eventNames       return events with any of these event names.
    * @param targetEntityType return events of this targetEntityType:
    *   - None means no restriction on targetEntityType
    *   - Some(None) means no targetEntityType for this event
    *   - Some(Some(x)) means targetEntityType should match x.
    * @param targetEntityId   return events of this targetEntityId
    *   - None means no restriction on targetEntityId
    *   - Some(None) means no targetEntityId for this event
    *   - Some(Some(x)) means targetEntityId should match x.
    * @param sc               Spark context
    * @return RDD[Event]
    */
  override def find(appId: Int,
                    channelId: Option[Int],
                    startTime: Option[DateTime],
                    untilTime: Option[DateTime],
                    entityType: Option[String],
                    entityId: Option[String],
                    eventNames: Option[Seq[String]],
                    targetEntityType: Option[Option[String]],
                    targetEntityId: Option[Option[String]])(sc: SparkContext): RDD[Event] = null

  /** :: DeveloperApi ::
    * Write events to database
    *
    * @param events    RDD of Event
    * @param appId     the app ID
    * @param channelId channel ID (default channel if it's None)
    * @param sc        Spark Context
    */
  override def write(events: RDD[Event],
                     appId: Int,
                     channelId: Option[Int])
                    (sc: SparkContext): Unit = {

  }

  @DeveloperApi
  override def delete(eventIds: RDD[String],
                      appId: Int,
                      channelId: Option[Int])(sc: SparkContext): Unit = {

  }
}
