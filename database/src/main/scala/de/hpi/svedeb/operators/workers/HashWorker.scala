package de.hpi.svedeb.operators.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Deploy, Props}
import akka.remote.RemoteScope
import de.hpi.svedeb.operators.HashJoinOperator.JoinSide
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor.{BuildPartitionedHashTable, BuiltPartitionedHashTable}
import de.hpi.svedeb.operators.workers.HashWorker.{HashJob, HashWorkerState, HashedTable}
import de.hpi.svedeb.operators.workers.PartitionHashWorker.{HashPartition, HashedPartitionKeys}
import de.hpi.svedeb.table.Table.{GetPartitions, PartitionsInTable}

object HashWorker {
  case class HashJob()
  case class HashedTable(result: Map[Int, ActorRef], side: JoinSide)

  private case class HashWorkerState(originalSender: ActorRef,
                                     expectedAnswerCount: Option[Int],
                                     answerCount: Int,
                                    // map of hash keys and the partitionWorkers that have it
                                     partitionWorkerMap: Map[Int, Seq[ActorRef]],
                                     resultMap: Map[Int, ActorRef]) {
    /*private def mergeMaps(map1: Map[Int, Seq[ActorRef]], map2: Map[Int, Seq[ActorRef]]): Map[Int, Seq[ActorRef]] = {
      map1.reduceLeft((b, pair) => pair._2.foldLeft{ case ()})
      map1.unionWith(map2, (_, seq1, seq2) => seq1 ++ seq2)
    }*/

    def storeSender(sender: ActorRef): HashWorkerState = {
      HashWorkerState(sender, expectedAnswerCount, answerCount, partitionWorkerMap, resultMap)
    }

    def storePartitionKeys(partitionKeys: Seq[Int], partitionHashWorker: ActorRef): HashWorkerState = {
      var mapCopy = partitionWorkerMap
      partitionKeys.foreach(key => {
        val list = mapCopy.getOrElse(key, Seq.empty) :+ partitionHashWorker
        mapCopy = mapCopy + (key -> list)
      })

      HashWorkerState(originalSender, expectedAnswerCount, answerCount + 1, mapCopy, resultMap)
    }

    def storePartitionCount(count: Int): HashWorkerState = {
      HashWorkerState(originalSender, Some(count), answerCount, partitionWorkerMap, resultMap)
    }

    def storeResultMap(resultMap: Map[Int, ActorRef]): HashWorkerState = {
      HashWorkerState(originalSender, expectedAnswerCount, answerCount, partitionWorkerMap, resultMap)
    }

    def storePHTA(hashKey: Int, sender: ActorRef): HashWorkerState = {
      val newMap = resultMap + (hashKey -> sender)
      HashWorkerState(originalSender, expectedAnswerCount, answerCount, partitionWorkerMap, newMap)
    }

    def isFinished: Boolean = {
      expectedAnswerCount.get == answerCount
    }

    def gotAllResults: Boolean = {
      resultMap.size == partitionWorkerMap.size
    }
  }

  def props(table: ActorRef, joinColumn: String, side: JoinSide): Props = Props(new HashWorker(table, joinColumn, side))
}

/**
  * An actor that hashes one side of a HashJoin, i.e. the left or the right input
  * @param table the input table actor
  * @param joinColumn the join column that needs to be hashed
  * @param side the side of the join, only used for the return value
  */
class HashWorker(table: ActorRef, joinColumn: String, side: JoinSide) extends Actor with ActorLogging {
  override def receive: Receive = active(HashWorkerState(ActorRef.noSender, None, 0, Map.empty, Map.empty))

  private def beginHashJob(state: HashWorkerState): Unit = {
    log.debug("Beginning hash job")
    context.become(active(state.storeSender(sender())))

    table ! GetPartitions()
  }

  private def handlePartitionsInTable(state: HashWorkerState, partitions: Map[Int, ActorRef]): Unit = {
    log.debug(s"handling partitions in table, #partition ${partitions.size}")
    context.become(active(state.storePartitionCount(partitions.size)))
    partitions.foreach(partition => {
      val worker = context.actorOf(PartitionHashWorker
        .props(partition._2, joinColumn)
        .withDeploy(new Deploy(RemoteScope(partition._2.path.address))), s"PartitionHashWorker${partition._1}")
      worker ! HashPartition()
    })
  }

  // PartitionHashWorker sent the keys that it produced during hashing.
  // We store information about which PartitionHashWorker has information for which hashKeys
  private def handleHashedPartitionKeys(state: HashWorkerState, hashes: Seq[Int]): Unit = {
    log.debug("handling hashed partitions keys")
    val newState = state.storePartitionKeys(hashes, sender())
    context.become(active(newState))

    if (newState.isFinished) {
      log.debug("All partitions have been hashed, now creating remote HashTables (aka PartitionHashTableActors)")
      newState.partitionWorkerMap.foreach{ case (hashKey, partitionWorkers) =>
        // Does not matter where this is created because the PHTA asks multiple partitionHashWorkers
        val hashTable = context.actorOf(PartitionedHashTableActor.props(hashKey, partitionWorkers),
          s"PartitionedHashTableActor$hashKey")
        hashTable ! BuildPartitionedHashTable()
      }
    }
  }

  private def handleBuiltPartitionedHashTable(state: HashWorkerState, hashKey: Int): Unit = {
    log.debug(s"storing PHTA for key $hashKey")
    val newState = state.storePHTA(hashKey, sender())
    context.become(active(newState))

    if (newState.gotAllResults) {
      log.debug("got all results")
      newState.originalSender ! HashedTable(newState.resultMap, side)
    }
  }

  private def active(state: HashWorkerState): Receive = {
    case HashJob() => beginHashJob(state)
    case PartitionsInTable(partitions) => handlePartitionsInTable(state, partitions)
    case HashedPartitionKeys(hashKeys) => handleHashedPartitionKeys(state, hashKeys)
    case BuiltPartitionedHashTable(hashKey) => handleBuiltPartitionedHashTable(state, hashKey)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
