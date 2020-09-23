## Spark源码分析(六) cache,persist,checkpoint

### cache

> 测试案例
>
> ```scala
>  def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-cache")
>     val sc = new SparkContext(conf)
>     val sourceRDD: RDD[String] = sc.textFile("in/word*.txt")
>     val flatMapRDD = sourceRDD.flatMap(x => x.split(" "))
>     val mapRDD = flatMapRDD.map(x => (x, 1))
>     val resRDD = mapRDD.reduceByKey(_ + _)
>     val cache: resRDD.type = resRDD.cache()
>     // 测试缓存的效率
>     resRDD.saveAsTextFile("output-cache")
>     resRDD.saveAsTextFile("output-cache2")
>     // 让程序睡一会儿   便于查看详细信息
>     Thread.sleep(Long.MaxValue)
>     sc.stop()
> ```

我们在本地测试该wordcount案例，我们知道saveAsTextFile是行动会触发job的提交，根据我们的代码我们第一次保存文件时触发cache，我们看到cache方法的返回值的类型是他本身，说明他不是算子。我们在进行一次文件的保存，我们可以看本地localhost:4040网站看他运行效率。

![截屏2020-09-23 下午2.53.23](/Users/mac/Documents/截屏2020-09-23 下午2.53.23.png)

我们看到第二次的执行效率远大于第一次进行保存的时间。我们在看其详细的DAG图我们可以看到在执行前我们是调用了上次缓存好的数据。

![截屏2020-09-23 下午2.54.44](/Users/mac/Documents/截屏2020-09-23 下午2.54.44.png)

我们来看源码，一级一级进入cache默认调用persist并且存储级别为MEMORY_ONLY，

```scala
/**
 * Persist this RDD with the default storage level (`MEMORY_ONLY`).
 */
def cache(): this.type = persist()

def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // 如果用户已经持久化这个RDD，我们就需要改变RDD的持久化级别为当前这个级别
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }
```

进入最后一行代码的persist方法

```scala
private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
  // TODO: Handle changes of StorageLevel  
  //storageLevel 默认为StorageLevel.NONE
  // 如果持久化级别为none，并且新的level和之前的level不同 并且不运行覆盖
  if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
    throw new UnsupportedOperationException(
      "Cannot change storage level of an RDD after it was already assigned a level")
  }
  // If this is the first time this RDD is marked for persisting, register it
  // with the SparkContext for cleanups and accounting. Do this only once.
  // storageLevel默认为StorageLevel.NONE 说明这是第一次进行持久化 就将当前RDD进行一个标记，并修改级别
  if (storageLevel == StorageLevel.NONE) {
    sc.cleaner.foreach(_.registerRDDForCleanup(this))
    sc.persistRDD(this)
  }
  storageLevel = newLevel
  this
}
```

经过以上的内容，我们可以知道，cache会在执行一次action后将数据缓存到内存当中，如果内存不够存储，就回将数据按分区缓存部分数据。

### persist

我们从上面的内容可以知道cache的底层调用了persist，其中他的持久化级别为MEMORY_ONLY，我们可以看一下具体有多少级别。

```scala
// 第一个参数 是否使用磁盘   第二个参数 使用内存  第三个参数  使用堆外内存  第四个参数 是否序列化 
// 第五个参数  复本数量
val NONE = new StorageLevel(false, false, false, false)
val DISK_ONLY = new StorageLevel(true, false, false, false)
val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
val MEMORY_ONLY = new StorageLevel(false, true, false, true)
val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
val OFF_HEAP = new StorageLevel(true, true, true, false, 1)
```

> 测试案例
>
> ```scala
> def main(args: Array[String]): Unit = {
>   val conf = new SparkConf().setMaster("local[*]").setAppName("spark-persist")
>   val sc = new SparkContext(conf)
> 
>   val sourceRDD: RDD[String] = sc.textFile("in/word*.txt")
>   val flatMapRDD = sourceRDD.flatMap(x => x.split(" "))
>   val mapRDD = flatMapRDD.map(x => (x, 1))
>   val resRDD = mapRDD.reduceByKey(_ + _)
>   //指定持久化级别
>   val cache: resRDD.type = resRDD.persist(StorageLevel.DISK_ONLY)
>   // 测试缓存的效率
>   resRDD.saveAsTextFile("output-persist1")
>   resRDD.saveAsTextFile("output-persist2")
>   // 让程序睡一会儿   便于查看详细信息
>   Thread.sleep(Long.MaxValue)
>   sc.stop()
> }
> ```

我们查看图形化界面的storge，可以看到他的storge level。

![截屏2020-09-23 下午3.50.12](/Users/mac/Documents/截屏2020-09-23 下午3.50.12.png)

### checkpoint

> 测试案例
>
> ```scala
>  def main(args: Array[String]): Unit = {
>     val conf = new SparkConf().setMaster("local[*]").setAppName("spark-check")
>     val sc = new SparkContext(conf)
> 		// 设置检查点存放的目录
>     val sourceRDD: RDD[String] = sc.textFile("in/user_info.txt")
>     sc.setCheckpointDir("output-check1")
>     val filterRDD: RDD[String] = sourceRDD.filter(_.length > 5)
>    	// 设置检查点	
>     filterRDD.checkpoint()
>    	// 第一次保存
>     filterRDD.saveAsTextFile("output-check2")
> 		// 第二次保存
>     filterRDD.saveAsTextFile("output-check3")
>     Thread.sleep(Long.MaxValue)
>     sc.stop()
>   }
> ```

我们在上面代码中只进行了俩个行动算子，但是却出现了3个job。我们在分开看这个三个job的具体内容

![截屏2020-09-23 下午5.53.03](/Users/mac/Documents/截屏2020-09-23 下午5.53.03.png)

- job 0

![截屏2020-09-23 下午5.54.51](/Users/mac/Documents/截屏2020-09-23 下午5.54.51.png)

- job 1

![截屏2020-09-23 下午5.56.43](/Users/mac/Documents/截屏2020-09-23 下午5.56.43.png)

- job 2

![截屏2020-09-23 下午5.57.35](/Users/mac/Documents/截屏2020-09-23 下午5.57.35.png)

从上面的图，我们可以知道在本次的测试代码中，在到checkPoint点时，有一个job去将数据存储在磁盘，另一个job继续向下执行。等到下一个行动算子，直接从checkpoint点开始执行。

我们对上面的代码进行一些修改，在设置检查点前对rdd进行cache，这样就可以避免重复计算，而且在以后的使用中会优先使用cache中的数据。

```scala
 def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-check")
    val sc = new SparkContext(conf)
		// 设置检查点存放的目录
    val sourceRDD: RDD[String] = sc.textFile("in/user_info.txt")
    sc.setCheckpointDir("output-check1")
    val filterRDD: RDD[String] = sourceRDD.filter(_.length > 5)
   	filterRDD.cache()
   	// 设置检查点	
    filterRDD.checkpoint()
   	// 第一次保存
    filterRDD.saveAsTextFile("output-check2")
		// 第二次保存
    filterRDD.saveAsTextFile("output-check3")
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
```

