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
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.collection.immutable.{Queue, SortedMap}

import akka.actor._
import journal.io.api._

import org.eligosource.eventsourced.core._
import org.eligosource.eventsourced.util.JavaSerializer

class JournalioJournal(dir: File)(implicit system: ActorSystem) extends Actor {

  // TODO: make configurable
  val serializer = new JavaSerializer[AnyRef]

  val inputWriteMsgQueue = new InputWriteMsgQueue
  val outputWriteMsgCache = new OutputWriteMsgCache

  val disposer = Executors.newSingleThreadScheduledExecutor()
  val journal = new Journal

  var commandListener: Option[ActorRef] = None
  var counter = 0L

  def receive = {
    case cmd: WriteMsg => {
      val c = if(cmd.genSequenceNr) cmd.forSequenceNr(counter) else cmd
      val m = c.message.copy(sender = None) // message to be written

      // write input or output message
      val loc = journal.write(serializer.toBytes(c.copy(message = m, target = null)), Journal.WriteType.SYNC)

      // optionally, add output message (written by reliable output channel) to cache
      if (c.channelId != Channel.inputChannelId) {
        outputWriteMsgCache.update(c, loc)
      }

      // optionally, write acknowledgement
      c.ackSequenceNr.foreach { snr =>
        journal.write(serializer.toBytes(WriteAck(c.componentId, c.channelId, snr)), Journal.WriteType.SYNC)
      }

      if (c.target != context.system.deadLetters) c.target ! c.message
      if (sender   != context.system.deadLetters) sender ! Ack

      counter = m.sequenceNr + 1
      commandListener.foreach(_ ! cmd)
    }
    case cmd: WriteAck => {
      journal.write(serializer.toBytes(cmd), Journal.WriteType.SYNC)
      if (sender != context.system.deadLetters) sender ! Ack
      commandListener.foreach(_ ! cmd)
    }
    case cmd: DeleteMsg => {
      outputWriteMsgCache.update(cmd).foreach(journal.delete)

      if (sender != context.system.deadLetters) sender ! Ack
      commandListener.foreach(_ ! cmd)
    }
    case BatchReplayInput(replays) => {
      val starts = replays.foldLeft(Map.empty[Int, (Long, ActorRef)]) { (a, r) =>
        a + (r.componentId -> (r.fromSequenceNr, r.target))
      }
      replayInputMessages { (cmd, acks) =>
        starts.get(cmd.componentId) match {
          case Some((fromSequenceNr, target)) if (cmd.message.sequenceNr >= fromSequenceNr) => {
            target ! cmd.message.copy(sender = None, acks = acks)
          }
          case _ => {}
        }
      }
      if (sender != context.system.deadLetters) sender ! Ack
    }
    case r @ ReplayInput(compId, fromNr, target) => {
      // call receive directly instead sending to self
      // (replay must not interleave with other commands)
      receive(BatchReplayInput(List(r)))
    }
    case ReplayOutput(compId, chanId, fromNr, target) => {
      replayOutputMessages(compId, chanId, fromNr, msg => target ! msg.copy(sender = None))
      if (sender != context.system.deadLetters) sender ! Ack
    }
    case GetCounter => {
      sender ! getCounter
    }
    case SetCommandListener(cl) => {
      commandListener = cl
    }
  }

  def replayInputMessages(p: (WriteMsg, List[Int]) => Unit) {
    journal.redo().asScala.foreach { location =>
      serializer.fromBytes(location.getData) match {
        case cmd: WriteMsg if (cmd.channelId == Channel.inputChannelId) => {
          inputWriteMsgQueue.enqueue(cmd)
        }
        case cmd: WriteMsg => {
          outputWriteMsgCache.update(cmd, location)
        }
        case cmd: WriteAck => {
          inputWriteMsgQueue.ack(cmd)
        }
      }
      if (inputWriteMsgQueue.size > 20000 /* TODO: make configurable */ ) {
        val (cmd, acks) = inputWriteMsgQueue.dequeue(); p(cmd, acks)
      }
    }
    inputWriteMsgQueue.foreach { ca => p(ca._1, ca._2) }
    inputWriteMsgQueue.clear()
  }

  def replayOutputMessages(componentId: Int, channelId: Int, fromSequenceNr: Long, p: Message => Unit) {
    outputWriteMsgCache.messages(componentId, channelId, fromSequenceNr).foreach(p)
  }

  def getCounter: Long = {
    val cmds = journal.undo().asScala.map { location => serializer.fromBytes(location.getData) }
    val cmdo = cmds.collectFirst { case cmd: WriteMsg => cmd }
    cmdo.map(_.message.sequenceNr + 1).getOrElse(1L)
  }

  override def preStart() {
    dir.mkdirs()

    journal.setPhysicalSync(false)
    journal.setDirectory(dir)
    journal.setWriter(system.dispatcher)
    journal.setDisposer(disposer)
    journal.setChecksum(false)
    journal.open()

    counter = getCounter
  }

  override def postStop() {
    journal.close()
    disposer.shutdown()
  }
}

/**
 * Queue for input WriteMsg commands including a mechanism for matching
 * corresponding acknowledgements.
 */
private [journal] class InputWriteMsgQueue extends Iterable[(WriteMsg, List[Int])] {
  var cmds = Queue.empty[WriteMsg]
  var acks = Map.empty[Key, List[Int]]

  var len = 0

  def enqueue(cmd: WriteMsg) {
    cmds = cmds.enqueue(cmd)
    len = len + 1
  }

  def dequeue(): (WriteMsg, List[Int]) = {
    val (cmd, q) = cmds.dequeue
    val key = Key(cmd.componentId, cmd.channelId, cmd.message.sequenceNr, 0)
    cmds = q
    len = len - 1
    acks.get(key) match {
      case Some(as) => { acks = acks - key; (cmd, as) }
      case None     => (cmd, Nil)
    }
  }

  def ack(cmd: WriteAck) {
    val key = Key(cmd.componentId, Channel.inputChannelId, cmd.ackSequenceNr, 0)
    acks.get(key) match {
      case Some(as) => acks = acks + (key -> (cmd.channelId :: as))
      case None     => acks = acks + (key -> List(cmd.channelId))
    }
  }

  def iterator =
    cmds.iterator.map(c => (c, acks.getOrElse(Key(c.componentId, c.channelId, c.message.sequenceNr, 0), Nil)))

  override def size =
    len

  def clear() {
    acks = Map.empty
    cmds = Queue.empty
    len = 0
  }
}

/**
 * Cache for output WriteMsg commands.
 */
private [journal] class OutputWriteMsgCache {
  var cmds = SortedMap.empty[Key, (Location, WriteMsg)]

  def update(cmd: WriteMsg, loc: Location) {
    val key = Key(cmd.componentId, cmd.channelId, cmd.message.sequenceNr, 0)
    cmds = cmds + (key -> (loc, cmd))
  }

  def update(cmd: DeleteMsg): Option[Location] = {
    val key = Key(cmd.componentId, cmd.channelId, cmd.msgSequenceNr, 0)
    cmds.get(key) match {
      case Some((loc, msg)) => { cmds = cmds - key; Some(loc) }
      case None             => None
    }
  }

  def messages(componentId: Int, channelId: Int, fromSequenceNr: Long): Iterable[Message] = {
    val from = Key(componentId, channelId, fromSequenceNr, 0)
    val to = Key(componentId, channelId, Long.MaxValue, 0)
    cmds.range(from, to).values.map(_._2.message)
  }
}
