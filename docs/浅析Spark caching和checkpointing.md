# 浅析Apache Spark Caching和Checkpointing

#### by [谭杨@MapLeap](https://github.com/tyxx11)

作为一个Apache Spark应用开发人员，内存管理是最重要的人物之一，但cacheing和checkpointing之间的差异可能会导致混乱。这2种操作是都是用来防止rdd(弹性分布式数据集)每次被引用时被重复计算带来的时间和空间上不必要的损失。然而他们之间的区别是什么呢？

### Caching

cache 机制保证了需要访问重复数据的应用（如迭代型算法和交互式应用）可以运行的更快。有多种级别的持久化策略让开发者选择，使开发者能够对空间和计算成本进行权衡，同时能指定out of memory时对rdd的操作（缓存在内存或者磁盘，并且可以指定在内存不够的情况下按照FIFO的策略选取一部分block交换到磁盘来产生空余空间）。因此Spark不但可以对rdd重复计算还能在节点发生故障时重新计算丢失的分区。最后，被缓存的rdd存在于一个running的应用的生命周期内，如果这个应用终止了，那么缓存的rdd也会同时被删除。

### Checkpointing

checkpointing把rdd存储到一个可靠的存储系统（例如HDFS,S3）。checkpoint一个rdd有点类似于Hadoop中把中间计算结果存储到磁盘，损失部分执行性能来获得更好的从运行过程中出现failures时recover的能力。因为rdd是checkpoint在外部的存储系统（磁盘，HDFS,S3等），所以checkpoint过的rdd能够被其他的应用重用。

### caching和checkpointing的联系

我们先来看rdd的计算路径来了解caching和checkpointing的相互作用。
Spark engine的核心是[DAGScheduler](https://github.com/apache/spark/blob/dcf8a9f331c6193a62bbc9282bdc99663e23ca19/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala)。它把一个spark job分解成由若干个stages组成的DAG。每一个shuffle或者result stage再分解成一个个在RDD的分区中独立运行的task。一个RDD的iterator方法是一个task访问基础数据分区的入口：

```scala
/**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
*/	
 final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      getOrCompute(split, context)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
```
我们可以从代码中看到，如果设置了存储级别，表明rdd可能被缓存，它首先尝试调用getOrCompute方法从block manager中得到分区。

```scala
/**
   * Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached.
   */
  private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    // This method is called on executors, so we need call SparkEnv.get instead of sc.env.
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
      readCachedBlock = false
      computeOrReadCheckpoint(partition, context)
    }) match {
      case Left(blockResult) =>
        if (readCachedBlock) {
          val existingMetrics = context.taskMetrics().registerInputMetrics(blockResult.readMethod)
          existingMetrics.incBytesReadInternal(blockResult.bytes)
          new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
            override def next(): T = {
              existingMetrics.incRecordsReadInternal(1)
              delegate.next()
            }
          }
        } else {
          new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
        }
      case Right(iter) =>
        new InterruptibleIterator(context, iter.asInstanceOf[Iterator[T]])
    }
  }
```

如果block manager中没有这个rdd的分区，那么它就去computeOrReadCheckpoint:

```scala
/**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
      compute(split, context)
    }
  }
```

正如你猜测的一样，这个computeOrReadCheckpoint这个方法会从checkpoint中寻找对应的数据，如果rdd没有被checkpoint，那么就从当前计算的分区开始计算。

### 是时候聊聊他们的区别了

cache 机制是每计算出一个要 cache 的 partition 就直接将其 cache 到内存了。但是checkpoint 没有使用这种第一次计算得到就存储的方法，而是等到 job 结束后另外启动专门的 job 去完成 checkpoint 。也就是说需要 checkpoint 的 RDD 会被计算两次。因此，在使用 rdd.checkpoint() 的时候，建议加上 rdd.cache()，这样第二次运行的 job 就不用再去计算该 rdd 了，直接读取 cache 写磁盘。其实 Spark 提供了 rdd.persist(StorageLevel.DISK_ONLY) 这样的方法，相当于 cache 到磁盘上，这样可以做到 rdd 第一次被计算得到时就存储到磁盘上，但这个 persist 和 checkpoint 有很多不同。前者虽然可以将 RDD 的 partition 持久化到磁盘，但该 partition 由 blockManager 管理。一旦 driver program 执行结束，也就是 executor 所在进程 CoarseGrainedExecutorBackend stop，blockManager 也会 stop，被 cache 到磁盘上的 RDD 也会被清空（整个 blockManager 使用的 local 文件夹被删除）。而 checkpoint 将 RDD 持久化到 HDFS 或本地文件夹，如果不被手动 remove 掉，是一直存在的，也就是说可以被下一个 driver program 使用，而 cached RDD 不能被其他 dirver program 使用。

### 总结

使用checkpoint会消耗更多的时间在rdd的读写上（因为要使用外部存储系统HDFS,S3，或者磁盘），但是Spark worker的一些failures不一定导致重新计算。另一方面，caching的rdd 不会永久占用存储空间，但是重新计算在Spark worker出现一些failures的时候是必要的。综上，这2个东东都是取决于开发者自己的角度结合业务场景来使用，一般情况下，综合计算任务的性能来进行2者的选择（tips:大部分情况用cache就够了，如果感觉 job 可能会出错可以手动去 checkpoint 一些 critical 的 RDD）。








