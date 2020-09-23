## Spark源码分析(三) RDD转换算子

### groupByKey

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-groupByKey")
>     val sc = new SparkContext(conf)
>     val sourceRDD: RDD[String] = sc.makeRDD(List(
>       "spark", "hadoop", "hive", "spark",
>       "spark", "spark", "flink", "hbase",
>       "kafka", "kafka", "kafka", "kafka",
>       "hadoop", "flink", "hive", "flink"
>     ), 4)
>     val mapRDD: RDD[(String, Int)] = sourceRDD.map((_, 1))
>     // val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
>   	val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey(5)
>     groupRDD.saveAsTextFile("output1")
>     sc.stop()
>   }
> ```

1. 我们首先进入groupByKey()方法，然后搜索groupByKey(),我们会发现有三个方法的重载。查看下面的代码，最终其实都是调用了最后一个方法，需要传入一个Partitioner分区器。

- 如果我们不传入值，我们会拿到父RDD的默认Partitioner分区器。
- 传入一个数值，会根据这个值创建一个HashPartitioner
- 我们也可以直接传入一个Partitioner对象

```scala
def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(defaultPartitioner(self))
  }

def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])] = self.withScope {
    groupByKey(new HashPartitioner(numPartitions))
  }

def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }
```

2. spark框架默认实现了俩个分区器HashPartitioner和RangePartitioner。根据我们的测试案例,`groupByKey(new HashPartitioner(numPartitions)) `找到此行，打开HashPartitioner类，找到getPartition方法，我们发现他主要调用了`Utils.nonNegativeMod(key.hashCode, numPartitions)`,在进入到该方法。

   ```scala
   //我们会发现，他的实现是使用key值的hashCode对我们传入分区数进行取模
   def nonNegativeMod(x: Int, mod: Int	): Int = {
       val rawMod = x % mod
       rawMod + (if (rawMod < 0) mod else 0)
     }
   ```

3. 下面我们看groupByKey的大致实现

```scala
def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
  // 创建一个 CompactBuffer 
    val createCombiner = (v: V) => CompactBuffer(v)
  // 将值都加入到 CompactBuffer中
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
  // 将CompactBuffer进行合并
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[RDD[(K, Iterable[V])]]
  }
```

- CompactBuffer  我们点击进入CompactBuffer类,我们就可以把他当做一个集合

```scala
private[spark] class CompactBuffer[T: ClassTag] extends Seq[T] with Serializable 
```

- 进入到`combineByKeyWithClassTag`方法

```scala
//首先他会看key的类型是不是一个集合类型
if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("HashPartitioner cannot partition array keys.")
      }
    }
//创建一个Aggregator对象
    val aggregator = new Aggregator[K, V, C](
      // clean 进行序列化检测
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
// 判断父RDD的分区器和当前的分区其是否相同
    if (self.partitioner == Some(partitioner)) {
      // 如果相同 就可以直接调用父级的mapPartitions进行写入了
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      // 如果不同就会涉及到shuffle过程
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  
```

4. 根据我们的案例，我们可以修改groupByKey方法的参数进行测试

```scala
 //测试一   不设置分区，是否与默认相同 
val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
//测试二    设置与父级RDD一致
val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey(4)
//测试二    设置与父级RDD不同
val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey(4)
```

### groupBy

> 测试案例
>
> ```scala
>  def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-groupByKey")
>     val sc = new SparkContext(conf)
>     val sourceRDD: RDD[String] = sc.makeRDD(List(
>       "spark", "hadoop", "hive", "spark",
>       "spark", "spark", "flink", "hbase",
>       "kafka", "kafka", "kafka", "kafka",
>       "hadoop", "flink", "hive", "flink"
>     ), 4)
> 
>     val mapRDD: RDD[(String, Int)] = sourceRDD.map((_, 1))
>     val groupByRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)
>     groupByRDD.saveAsTextFile("output2")
>     sc.stop()
>   }
> ```

- 对比groupByKey和groupBy生成的数据

```
groupByKey
(spark,CompactBuffer(1, 1, 1, 1))
(kafka,CompactBuffer(1, 1, 1, 1))
(hbase,CompactBuffer(1))
groupBy
(hive,CompactBuffer((hive,1), (hive,1)))
(flink,CompactBuffer((flink,1), (flink,1), (flink,1)))
```

- 查看groupBy的源码,先对当前RDD进行了map操作然后调用了groupByKey方法

```scala
def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
    : RDD[(K, Iterable[T])] = withScope {
  val cleanF = sc.clean(f)
  this.map(t => (cleanF(t), t)).groupByKey(p)
}
```

### reduceByKey

> 测试案例
>
> ```scala
> 
>   def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-reduceByKey")
>     val sc = new SparkContext(conf)
>     val sourceRDD: RDD[String] = sc.makeRDD(List(
>       "spark", "hadoop", "hive", "spark",
>       "spark", "spark", "flink", "hbase",
>       "kafka", "kafka", "kafka", "kafka",
>       "hadoop", "flink", "hive", "flink"
>     ), 4)
> 
>     val mapRDD: RDD[(String, Int)] = sourceRDD.map((_, 1))
>     val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
> 
>     reduceByKeyRDD.saveAsTextFile("output-reduceByKey")
>     Thread.sleep(
>       Long.MaxValue
>     )
>     sc.stop()
>   }
> ```

我们进入到reduceBykey方法中,我们可以看到，与上面groupByKey的底层一致，都是底层还是调用了combineByKeyWithClassTag方法，通过下面的源代码，我们就可以理解reduceBykey,我们使用reduceByKey时传入了一个函数，底层将其应用到分区内部和分区间的运算。

```scala
def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  reduceByKey(defaultPartitioner(self), func)
}
def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }
 def combineByKeyWithClassTag[C](
      createCombiner: V => C,   //创建一个存放key的集合
      mergeValue: (C, V) => C,  //分区内部进行运算
      mergeCombiners: (C, C) => C, // 分区间运算合并
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)
```

### foldByKey

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-foldByKey")
>     val sc = new SparkContext(conf)
> 
>     val sourceRDD: RDD[(String, Int)] = sc.makeRDD(List(
>       ("spark", 1), ("hadoop", 2), ("scala", 2), ("hive", 3),
>       ("spark", 3), ("hadoop", 1), ("scala", 4), ("hive", 2)
>     ),2)
>     val foldRDD: RDD[(String, Int)] = sourceRDD.foldByKey(0)(math.max)
>   	//val foldRDD: RDD[(String, Int)] = sourceRDD.foldByKey(10)(math.max)
>     foldRDD.saveAsTextFile("output-foldByKey")
>     sc.stop()
>   }
> ```

foldByKey的作用与reduceByKey类似，我们可以看一下他的源码。我们首先就可以看到 `combineByKeyWithClassTag`首先在底层调用的方法与之前的一致，同时将我们传入的方法应用于分区内和分区间。主要注意下面俩个注释，这个传入的值会与在分区内部参与计算

```scala
def foldByKey(
      zeroValue: V,
      partitioner: Partitioner)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
  // 将zeroValue序列化为一个字节数组，就能在每一个key使用时拿到复本
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    // When deserializing, use a lazy val to create just one instance of the serializer per task
  //反序列化的时候 用一个lazy的值创建唯一一个序列化的值为每一个task
    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[V](ByteBuffer.wrap(zeroArray))

    val cleanedFunc = self.context.clean(func)
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)
  }
```

### aggregateByKey

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>   val conf = new SparkConf().setMaster("local[*]").setAppName("spark-aggregateByKey")
>   val sc = new SparkContext(conf)
> 
>   val sourceRDD: RDD[(String, Int)] = sc.makeRDD(List(
>     ("spark", 1), ("hadoop", 2), ("scala", 2), ("hive", 3),
>     ("spark", 3), ("hadoop", 1), ("scala", 11), ("hive", 2),
>     ("hive", 1)
>   ),3)
>   val foldRDD: RDD[(String, Int)] = sourceRDD.aggregateByKey(10)(_+_,_+_)
>   foldRDD.saveAsTextFile("output-aggregateByKey")
>   sc.stop()
> }
> ```

aggregateByKey与foldByKey类似，但是要比foldByKey更灵活，不仅可以指定分区内部的计算规则，也可以指定分区间的计算规则。注意最后一行combineByKeyWithClassTag方法的传入的函数。

```scala
def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOp: (U, V) => U,
      combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
    val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

    // We will clean the combiner closure later in `combineByKey`
    val cleanedSeqOp = self.context.clean(seqOp)
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)
  }
```

### distinct

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-distinct")
>     val sc = new SparkContext(conf)
> 
>     val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 2, 4), 2)
>     val disRDD: RDD[Int] = sourceRDD.distinct()
>     println(disRDD.collect().toBuffer)
>     sc.stop()
>   }
> ```

distinct主要进行去重操作，我们可看一下他底层是怎么实现的

```scala
partitioner match {
    case Some(_) if numPartitions == partitions.length =>
    mapPartitions(removeDuplicatesInPartition, preservesPartitioning = true)
    case _ => map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
 }
```

我们可以按照测试案例，对数据进行一次模拟运算，看其具体是怎么实现去重的功能。

```scala
(1, 2, 3, 4, 2, 4)   经过map(x => (x, null))
(1，null),(2，null),(3，null),(4，null),(2，null),(4，null)  
	经过reduceByKey((x, _) => x) 分区内部与分区间进行聚合
(1，null),(2，null),(3，null),(4，null)  在调用map(_._1)就可以实现我们去重的效果
1，2，3，4
```

