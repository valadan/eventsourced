/*
 * Copyright 2012 Eligotech BV.
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
package org.eligosource.eventsourced.journal

import java.io.File
import java.nio.ByteBuffer

import akka.actor._

import org.fusesource.leveldbjni.JniDBFactory._
import org.iq80.leveldb._

import org.eligosource.eventsourced.core._
import org.eligosource.eventsourced.util.JavaSerializer

/**
 * LevelDB based journal.
 */
class LeveldbJournal(dir: File) extends Actor {
  import LeveldbJournal._

  // TODO: make configurable
  private val serializer = new JavaSerializer[Message]

  val levelDbReadOptions = new ReadOptions
  val levelDbWriteOptions = new WriteOptions().sync(false)
  val leveldb = factory.open(dir, new Options().createIfMissing(true))

  var commandListener: Option[ActorRef] = None
  var counter = 0L

  def receive = {
    case cmd: WriteMsg => {
      val c = if(cmd.genSequenceNr) cmd.forSequenceNr(counter) else cmd
      store(c)
      if (c.target != context.system.deadLetters) c.target ! c.message
      if (sender   != context.system.deadLetters) sender ! Ack
      commandListener.foreach(_ ! cmd)
    }
    case cmd: WriteAck => {
      store(cmd)
      if (sender != context.system.deadLetters) sender ! Ack
      commandListener.foreach(_ ! cmd)
    }
    case cmd: DeleteMsg => {
      store(cmd)
      if (sender != context.system.deadLetters) sender ! Ack
      commandListener.foreach(_ ! cmd)
    }
    case BatchReplayInput(replays) => {
      // call receive directly instead sending to self
      // (replay must not interleave with other commands)
      replays.foreach(receive)
    }
    case ReplayInput(compId, fromNr, target) => {
      replay(compId, Channel.inputChannelId, fromNr, msg => target ! msg.copy(sender = None))
      if (sender != context.system.deadLetters) sender ! Ack
    }
    case ReplayOutput(compId, chanId, fromNr, target) => {
      replay(compId, chanId, fromNr, msg => target ! msg.copy(sender = None))
      if (sender != context.system.deadLetters) sender ! Ack
    }
    case GetCounter => {
      sender ! getCounter
    }
    case SetCommandListener(cl) => {
      commandListener = cl
    }
  }

  def store(cmd: WriteMsg) {
    val batch = leveldb.createWriteBatch()
    val msg = cmd.message
    try {
      // add message to batch
      counter = msg.sequenceNr
      val k = Key(cmd.componentId, cmd.channelId, msg.sequenceNr, 0)
      val m = msg.copy(sender = None)
      batch.put(CounterKeyBytes, counterToBytes(counter))
      batch.put(k.bytes, serializer.toBytes(m.copy(sender = None)))

      // optionally, add ack to batch
      cmd.ackSequenceNr.foreach { snr =>
        val k = Key(cmd.componentId, Channel.inputChannelId, snr, cmd.channelId)
        batch.put(k.bytes, Array.empty[Byte])
      }
      leveldb.write(batch, levelDbWriteOptions)
      counter = counter + 1
    } finally {
      batch.close()
    }
  }

  def store(cmd: WriteAck) {
    val k = Key(cmd.componentId, Channel.inputChannelId, cmd.ackSequenceNr, cmd.channelId)
    leveldb.put(k.bytes, Array.empty[Byte], levelDbWriteOptions)
  }

  def store(cmd: DeleteMsg) {
    val k = Key(cmd.componentId, cmd.channelId, cmd.msgSequenceNr, 0)
    leveldb.delete(k.bytes, levelDbWriteOptions)
  }

  override def preStart() {
    counter = getCounter
  }

  override def postStop() {
    leveldb.close()
  }

  private def getCounter = leveldb.get(CounterKeyBytes, levelDbReadOptions) match {
    case null  => 1L
    case bytes => bytesToCounter(bytes) + 1L
  }

  private def replay(componentId: Int, channelId: Int, fromSequenceNr: Long, p: Message => Unit): Unit = {
    val iter = leveldb.iterator(levelDbReadOptions.snapshot(leveldb.getSnapshot))
    try {
      val startKey = Key(componentId, channelId, fromSequenceNr, 0)
      iter.seek(startKey.bytes)
      replay(iter, startKey, p)
    } finally {
      iter.close()
    }
  }

  @scala.annotation.tailrec
  private def replay(iter: DBIterator, key: Key, p: Message => Unit): Unit = {
    if (iter.hasNext) {
      val nextEntry = iter.next()
      val nextKey = Key(nextEntry.getKey)
      assert(nextKey.confirmingChannelId == 0)
      if (key.componentId         == nextKey.componentId &&
        key.initiatingChannelId == nextKey.initiatingChannelId) {
        val msg = serializer.fromBytes(nextEntry.getValue)
        val channelIds = confirmingChannelIds(iter, nextKey, Nil)
        p(msg.copy(acks = channelIds))
        replay(iter, nextKey, p)
      }
    }
  }

  @scala.annotation.tailrec
  private def confirmingChannelIds(iter: DBIterator, key: Key, channelIds: List[Int]): List[Int] = {
    if (iter.hasNext) {
      val nextEntry = iter.peekNext()
      val nextKey = Key(nextEntry.getKey)
      if (key.componentId         == nextKey.componentId &&
        key.initiatingChannelId == nextKey.initiatingChannelId &&
        key.sequenceNr          == nextKey.sequenceNr) {
        iter.next()
        confirmingChannelIds(iter, nextKey, nextKey.confirmingChannelId :: channelIds)
      } else channelIds
    } else channelIds
  }

}

private object LeveldbJournal {
  val CounterKeyBytes = Key(0, 0, 0L, 0).bytes

  def counterToBytes(value: Long) =
    ByteBuffer.allocate(8).putLong(value).array

  def bytesToCounter(bytes: Array[Byte]) =
    ByteBuffer.wrap(bytes).getLong
}
