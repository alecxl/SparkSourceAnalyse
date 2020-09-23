## Spark源码分析(二) RDD转换算子(窄依赖)

###  窄依赖

父RDD的Partition最多被子RDD的一个Partition使用,我们以下面的map测试案例为例

![截屏2020-09-15 下午8.44.28](/Users/mac/Documents/截屏2020-09-15 下午8.44.28.png)

### map

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-map")
>     val sc = new SparkContext(conf)
>     val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
>     val mapRdd: RDD[Int] = sourceRdd.map(_ * 10)
>     mapRdd.collect().foreach(println)
>     sc.stop()
>   }
> ```

1. 进入map方法

clean方法是对我们map传入方法进行序列化检测，因为我们的运算是分布式的，所有的运算不可能都在一个机器上，我们如果在函数中使用了没有序列化的值，就不能通过网络传输到其他节点进行计算。

this 指向调用当前方法的RDD，map方法会返回一个新的RDD,所以this就是当前新RDD的父级RDD

```scala
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](
      this, 
			(_, _, iter) => iter.map(cleanF)) 
  // 我们在执行map方法时，会对每个分区执行的map，此时的迭代器iter里就是每个分区
  }
```

2. 进入`new MapPartitionsRDD`

真正的调用是在这个类中的compute方法

```scala
 override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
```

### mapPartitions

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-mapPartition")
>     val sc = new SparkContext(conf)
>     val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
>       val mapRdd: RDD[Int] = sourceRdd.mapPartitions(
>       iter => iter.filter( _ % 2 ==0)
>     )
>     mapRdd.collect().foreach(println)
>     sc.stop()
>   }
> ```

1. 我们知道RDD的map方法会对集合中的所有元素，或者每一行都执行一次。mapPartitions与他区别是他传入函数的参数是一个迭代器，将每个分区的所有数据放到一个迭代器中。我们在使用的时候就可以使用迭代器所拥有的方法，但是需要注意的是函数的返回值也必须是一个迭代器。与map不同本方法一个分区只会执行一次。

```scala
def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }
```

### mapPartitionsWithIndex

与mapPartitions的理解和用法一样，不同的是每次我们都可以拿到分区编号

```scala
def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-mapPartitionWithIndex")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd: RDD[(Int, Int)] = sourceRdd.mapPartitionsWithIndex(
      (index,iter) =>{
        val tuples = iter.map(i => {
          (index, i)
        })
        tuples
      }
    )
    mapRdd.collect().foreach(println)
    sc.stop()
  }
```

源码如下: 想比于mapPartitions只是该方法传入的函数的参数的不同，这个函数将分区编号和迭代器同时传入。

```scala
def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(index, iter),
      preservesPartitioning)
  }
```

### flatMap

flatMap 一般用来进行扁平化处理

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-filter")
>     val sc = new SparkContext(conf)
>     val sourceRdd: RDD[String] = sc.makeRDD(List("hello spark","hello hadoop", "hello spark"))
>     val flatMapRDD: RDD[String] = sourceRdd.flatMap(value => {
>       value.split(" ")
>     })
>     println(flatMapRDD.collect().mkString(","))
>     sc.stop()
>   }
> ```

flatMap的理解与之前的都类似，使用的是迭代器的flatMap方法

```scala
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.flatMap(cleanF))
  }
```

### filter

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-filter")
>     val sc = new SparkContext(conf)
>     val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
>     val mapRdd: RDD[Int] = sourceRdd.filter(_%2==0)
>     mapRdd.collect().foreach(println)
>     sc.stop()
>   }
> ```

filter的原理及其类似，只是规定传入的方法不同

1. 进入filter

```scala
// 传入函数的返回值类型为Boolean
def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }
```

