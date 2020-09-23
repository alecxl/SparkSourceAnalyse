## Spark源码分析(四) RDD转换算子

### cogroup

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>   val conf = new SparkConf().setMaster("local[*]").setAppName("spark-cogroup")
>   val sc = new SparkContext(conf)
>   val source1: RDD[(String, Int)] = sc.makeRDD(List(("spark", 2), ("hadoop", 3), ("scala", 2),("scala", 3)))
>   val source2: RDD[(String, Int)] = sc.makeRDD(List(("spark", 1), ("hive", 3), ("scala", 4)))
>   val coRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = source1.cogroup(source2)
>   println(coRDD.collect().toBuffer)
>   sc.stop()
> }
> // result :
> //ArrayBuffer((hadoop,(CompactBuffer(3),CompactBuffer())), (spark,(CompactBuffer(2),CompactBuffer(1))), (scala,(CompactBuffer(2, 3),CompactBuffer(4))), (hive,(CompactBuffer(),CompactBuffer(3))))
> ```

cogroup的作用是将2个RDD进行合并，我们通过上边的案例，可以发现他将俩个RDD根据key值进行了合并，将key对应的value在每一个RDD进行合并成一个迭代器对象，在2个RDD的迭代器对象合并成一个元组。我们可以看一下他的源码。在案例中的进入方法

```scala
// 1. 我们这里只传入了一个RDD，首先根据调用方法RDD和传入的RDD拿到分区器，我们进入defaultPartitioner
def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    cogroup(other, defaultPartitioner(self, other))
  }

// 2. 分区计算
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
  	// 将所有RDD放入到一个集合中
    val rdds = (Seq(rdd) ++ others)
  	// 把有分区器并且分区>0的过滤出来
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))
  	//判断这个集合是不是空，如果不是空就将判断其有没最大值
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }
  //如果设置了spark.default.parallelism 就将其赋值给defaultNumPartitions
  // 否则就使用rdds中分区数量最大的值去赋值
    val defaultNumPartitions = if (rdd.context.conf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }
    // If the existing max partitioner is an eligible one, or its partitions number is larger
    // than or equal to the default number of partitions, use the existing partitioner.
  //翻译上面的注释，如果存在一个最大的分区器，并且它是一个有资格的，或者他的分区数量大于等于默认的就是这个已经
  //存在的分区器,否则就使用默认的分区数量创建一个分区器
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      new HashPartitioner(defaultNumPartitions)
    }
  }

// 3. 分区数量计算好之后的，就看cogroup的具体实现，在点击cogroup方法，进入到下面的方法
 def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("HashPartitioner cannot partition array keys.")
    }
    val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
     //将俩个rdd每一个key对应的value转化成迭代器对象，放到集合中
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }
//4. 我们还可以的发现colgroup可以一次性最多聚合4个RDD，具体使用方式如下
  val coRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int], Iterable[Int]))] = source1.cogroup(source2,source1,source2)

```

### intersection

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>   val conf = new SparkConf().setMaster("local[*]").setAppName("spark-intersection")
>   val sc = new SparkContext(conf)
>   val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 5, 7),2)
>   val sourceRdd2: RDD[Int] = sc.makeRDD(List(2, 5, 6, 8, 10),2)
>   val interRdd: RDD[Int] = sourceRdd.intersection(sourceRdd2)
>   println(interRdd.collect().toBuffer)
>   sc.stop()
> }
> // result : ArrayBuffer(2, 5)
> ```

从上面我们可以看到intersection的方法的功能是对俩个RDD之间求并集，我们直接看源码

```scala
 def intersection(other: RDD[T]): RDD[T] = withScope {
    this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
        .filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
        .keys
  }
```

我们根据上面的源码，走一下我们例子

```scala
val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 5, 7),2)
val sourceRdd2: RDD[Int] = sc.makeRDD(List(2, 5, 6, 8, 10),2)
val interRdd: RDD[Int] = sourceRdd.intersection(sourceRdd2)

((1,null), (2,null), (3,null), (5,null), (7,null))    this.map(v => (v, null))
((2,null), (5,null), (6,null), (8,null), (10,null))    other.map(v => (v, null)
                                                                 
this.map(v => (v, null)).cogroup(other.map(v => (v, null)))
    ArrayBuffer((6,(CompactBuffer(),CompactBuffer(null))), (8,(CompactBuffer(),CompactBuffer(null))), (10,(CompactBuffer(),CompactBuffer(null))), (2,(CompactBuffer(null),CompactBuffer(null))), (1,(CompactBuffer(null),CompactBuffer())), (3,(CompactBuffer(null),CompactBuffer())), (7,(CompactBuffer(null),CompactBuffer())), (5,(CompactBuffer(null),CompactBuffer(null))))

filter { case (_, (leftGroup, rightGroup)) => leftGroup.nonEmpty && rightGroup.nonEmpty }
((2,(CompactBuffer(null),CompactBuffer(null))),(5,(CompactBuffer(null),CompactBuffer(null))))     
keys 
(2, 5)
//这样我们就拿到了结果
```

### join系列

join系列的方法类似于sql语言中的join，但是RDD的join只有等值链接。join系列的方法主要有join，leftOuterJoin

,rightOuterJoin,fullOuterJoin.

> 测试案例
>
> ```scala
>   def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-join")
>     val sc = new SparkContext(conf)
> 
>     val sourceRdd: RDD[(String, Int)] = sc.makeRDD(List(("scala",2),("hadoop",3),("hive",1)),2)
>     val sourceRdd2: RDD[(String, Int)] = sc.makeRDD(List(("scala",4),("hadoop",2),("spark",5),("hadoop",4)),2)
> 
>     val joinRdd: RDD[(String, (Int, Int))] = sourceRdd.join(sourceRdd2)
>     val leftOuterJoinRdd: RDD[(String, (Int, Option[Int]))] = sourceRdd.leftOuterJoin(sourceRdd2)
>     val rightOuterJoinRdd: RDD[(String, (Option[Int], Int))] = sourceRdd.rightOuterJoin(sourceRdd2)
>     val fullOuterJoinRdd: RDD[(String, (Option[Int], Option[Int]))] = sourceRdd.fullOuterJoin(sourceRdd2)
> 
>     println(joinRdd.collect().toBuffer)
>     println(leftOuterJoinRdd.collect().toBuffer)
>     println(rightOuterJoinRdd.collect().toBuffer)
>     println(fullOuterJoinRdd.collect().toBuffer)
> 
>     sc.stop()
>   }
> /*
> join实现的效果就是将俩个RDD中每个key对应value自己聚合到一起最终返回
> joinRdd result:
> ArrayBuffer((scala,(2,4)), (hadoop,(3,2)), (hadoop,(3,4)))
> leftOuterJoin 就是将后面的RDD的内容根据key值join到前面RDD的内容上,后面RDD中元素的key值在前面的RDD中没有将不会出现在结果中。如果前面RDD的key值，在后面RDD没有，将补充一个None，如果有使用Some包装
> leftOuterJoin result:
> ArrayBuffer((scala,(2,Some(4))), (hive,(1,None)), (hadoop,(3,Some(2))), (hadoop,(3,Some(4))))
> 与leftOuterJoin类似
> rightOuterJoin result:
> ArrayBuffer((scala,(Some(2),4)), (spark,(None,5)), (hadoop,(Some(3),2)), (hadoop,(Some(3),4)))
> 俩个RDD的元素都会出现
> fullOuterJoin result:
> ArrayBuffer((scala,(Some(2),Some(4))), (hive,(Some(1),None)), (spark,(None,Some(5))), (hadoop,(Some(3),Some(2))), (hadoop,(Some(3),Some(4))))
> */
> ```

我们直接来看其底层是怎么实现的

```scala
 /*
我们可以看到他们的实现方式，都首先进行了cogroup，根据我们的案例，我们得到下面结果
((scala,(CompactBuffer(2),CompactBuffer(4))), (hive,(CompactBuffer(1),CompactBuffer())), (spark,(CompactBuffer(),CompactBuffer(5))), (hadoop,(CompactBuffer(3),CompactBuffer(2, 4))))
接下来他们的操作都是flatMapValues 对values进行扁平化处理,他们的主要区别就是扁平化的方式不同
*/
def join[W](other: RDD[(K, W)], partitioner: Partitioner): RDD[(K, (V, W))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues( pair =>
      // 同时遍历元组中的俩个元素，如果元组内的的俩个迭代器有值 就向外写出
      for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w)
    )
 }


 def leftOuterJoin[W](
      other: RDD[(K, W)],
      partitioner: Partitioner): RDD[(K, (V, Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      // 左外链接 首先要看下第二个迭代器在是否为空，为空就补None输出
      if (pair._2.isEmpty) {
        pair._1.iterator.map(v => (v, None))
      } else {
        //与上面不同这里第二个迭代器中的内容要使用Some包一下
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, Some(w))
      }
    }
  }	
//与左外链接类似 要看元组第一个迭代器是否为空
 def rightOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], W))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues { pair =>
      if (pair._1.isEmpty) {
        pair._2.iterator.map(w => (None, w))
      } else {
        for (v <- pair._1.iterator; w <- pair._2.iterator) yield (Some(v), w)
      }
    }
  }

// 将上面三种情况结合起来 用模式匹配来实现
def fullOuterJoin[W](other: RDD[(K, W)], partitioner: Partitioner)
      : RDD[(K, (Option[V], Option[W]))] = self.withScope {
    this.cogroup(other, partitioner).flatMapValues {
      case (vs, Seq()) => vs.iterator.map(v => (Some(v), None))
      case (Seq(), ws) => ws.iterator.map(w => (None, Some(w)))
      case (vs, ws) => for (v <- vs.iterator; w <- ws.iterator) yield (Some(v), Some(w))
    }
  }
```

### repartition 和 coalesce

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>   val conf = new SparkConf().setMaster("local[*]").setAppName("spark-repartition")
>   val sc = new SparkContext(conf)
>   val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
>   val repartitionRDD: RDD[Int] = sourceRDD.repartition(3)
>   repartitionRDD.saveAsTextFile("output-repartition")
>   sc.stop()
> }
> ```

我们在拿到数据源时设置其分区数量为2，我们通过repartition方法去改变他的分区数量，我们看源码进入repartition方法

```scala
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
  coalesce(numPartitions, shuffle = true)
}
```

我们可以看到他底层调用了coalesce方法，并且传入了修改的分区数量，和是否shuffle为true进入coalesce方法

```scala

def coalesce(numPartitions: Int, shuffle: Boolean = false,
             partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
            (implicit ord: Ordering[T] = null)
    : RDD[T] = withScope {
  require(numPartitions > 0, s"Number of partitions ($numPartitions) must be positive.")
  // 根据本案例，我们使用repartition，默认shuffle为true
    if (shuffle) {
    /** Distributes elements evenly across output partitions, starting from a random partition. */
     //定义一个方法 ，从随机分区开始，在输出分区之间均匀分配元素
    val distributePartition = (index: Int, items: Iterator[T]) => {
      //这里说是随机，但其实并没有真的随机。我们点进去Random类，发现其底层是的java的Random类，
      // 我们知道当Random有参构造器与nextInt方法中参数值是确定的我们获得的随机数也是固定0-nextInt-1的
      //一个值,当我们修改index值，不同index值这样会产生不同的随机值。所以我们获得position其实是固定的
      //但是这样就可以将数据均匀的分配到不同的分区中
      var position = new Random(hashing.byteswap32(index)).nextInt(numPartitions)
      items.map { t =>
        // Note that the hash code of the key will just be the key itself. The HashPartitioner
        // will mod it with the number of total partitions.
        position = position + 1
        //返回（随机位置，迭代器数据）
        (position, t)
      }
    } : Iterator[(Int, T)]

    // include a shuffle step so that our upstream tasks are still distributed
    //用CoalescedRDD包装一个ShuffledRDD，并使用新的分区数量构建hash分区器和该值传入
    new CoalescedRDD(
      new ShuffledRDD[Int, T, T](
        mapPartitionsWithIndexInternal(disshibutePartition, isOrderSensitive = true),
        new HashPartitioner(numPartitions)),
      numPartitions,
      partitionCoalescer).values
  } else {
      // 不进行shuffle，用CoalescedRDD将原来的rdd直接包装
    new CoalescedRDD(this, numPartitions, partitionCoalescer)
  }
}
```

当我们扩大分区时，一般都需要shuffle，我们也可以使用repartition方法去缩小分区，我们可以看一下repartition方法源码的注释。

```scala
/**
 * Return a new RDD that has exactly numPartitions partitions.
 *
 * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
 * a shuffle to redistribute data.
 * 当我们想去减少这个rdd的数量，我们可以考虑使用coalesce方法，因为repartition方法默认就要进行shuffle
 * 但是当我们在减少分区时有时候是不需要进行shuffle。
 * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
 * which can avoid performing a shuffle.
 */
```

