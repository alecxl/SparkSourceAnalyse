## Spark源码分析(一) 从textFile分析分区

> textFile()

使用textFile() 我们可以读取hdfs文件系统和本地文件系统中文件，该函数第一个参数为文件路径，第二个参数为最小的分区数.

```scala
  def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] 
```

我们现在使用下面的案例对源码进行分析，我们现在去读取本地文件系统下的文件，我在文件夹`in`下有四个文件

word1.txt,word2.txt,word3.txt,word4.txt。大小分别为23b，23b，23b，823kb。

```scala
val sparkConf =
    new SparkConf().setMaster("local[*]").setAppName("spark")
val sparkContext = new SparkContext(sparkConf)
val fileRDD: RDD[String] = sparkContext.textFile("in") 
fileRDD.saveAsTextFile("out")
sparkContext.stop()
```

1. 首先在上面代码`val fileRDD: RDD[String] = sparkContext.textFile("in") `打上断点，现在开始调试，进入该方法，我们会看到下面代码

   ```scala
    def textFile(
         path: String,
         minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
       assertNotStopped()
       hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
         minPartitions).map(pair => pair._2.toString).setName(path)
     }
   ```

   在该行`  hadoopFile`打上断点，方便下次调试，进入此方法

2. 看下面代码

   ```scala
   def hadoopFile[K, V](
         path: String,
         inputFormatClass: Class[_ <: InputFormat[K, V]],
         keyClass: Class[K],
         valueClass: Class[V],
         minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope 
   ```

   我们对应上一段代码，传入的值

   - path : 就是要读文件路径

   - inputFormatClass： 我们使用的是TextInputFormat

   - keyClass ：在使用TextInputFormat进行读取时 我们使用偏移量来作为他的key，类型为LongWritable

   - valueClass：我们每一行为文本类型进行读取 ， Text

   - minPartitions ： 它使用的是默认值 defaultMinPartitions，我们点进去

     ```scala
     /**
       * Default min number of partitions for Hadoop RDDs when not given by user
       * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2
       * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
       */
       def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
     ```

     看作者注释，我们可以知道我们在未设置值时 defaultMinPartitions默认为2.

   3. 在`hadoopFile` 方法中找到`new HadoopRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)`此行，打上断点并进入该方法。进入找到`getPartitions`方法，在方法中找到`val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)`该行，打上断点并进入此方法，一直进入到`getSplits`该方法。

      我们现在分开去分析该方法中的代码：

      - ```scala
        long totalSize = 0;                           // compute total size
        for (FileStatus file: files) {                // check we have valid files
          if (file.isDirectory()) {
            throw new IOException("Not a file: "+ file.getPath());
          }
          totalSize += file.getLen();
        }
        // 这段代码会计算你要读取文件下所有文件的总大小 
        // 根据我要读取的文件 我们可以得到 totalSize为823009 注意我最后一个文件是823kb，其他文件的单位是b
        ```

      - ```scala
        // 对应上面调用该方法 我们知道numSplits是上面的默认最s分区2
        // 如果这里numSplits为0 也就是用户设置该值为0时，spark会将其改为1，没有分区是无法进行计算的
        // 根据我的调试情况 我们计算出goalSize为411504
        long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
        long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
              FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);
        // 点击minSplitSize 我们可以知道该值为1 最终minSize为1
        ```

      - ```scala
        // 开始循环遍历每一个文件
        for (FileStatus file: files) {
              Path path = file.getPath();
          // 拿到这个文件的长度
              long length = file.getLen();
              if (length != 0) {
                FileSystem fs = path.getFileSystem(job);
                BlockLocation[] blkLocations;
                if (file instanceof LocatedFileStatus) {
                  blkLocations = ((LocatedFileStatus) file).getBlockLocations();
                } else {
                  blkLocations = fs.getFileBlockLocations(file, 0, length);
                }
                // 判断这个文件是否是可切分的
                if (isSplitable(fs, path)) {
                  // 与Hadoop相同 本地块大小为32MB 集群默认128MB
                  long blockSize = file.getBlockSize();
                  //计算分割的大小 在该处打上断点，方便以后调试，然后进入该方法，
                  // 此处代码我们放到下面 此时splitSize为goalSize 411504
                  long splitSize = computeSplitSize(goalSize, minSize, blockSize);
        					// 拿到此文件长度 去进行计算
                  long bytesRemaining = length;
                  // 我们先看第一个文件过来 原始文件大小23 splitSize SPLIT_SLOP是系统设定的值1.1
                  
                  // SPLIT_SLOP的理解 假设我们现在文件大小5.4 要切割大小splitSize为5 
                  // 按照之前的计划，每5就要有一个切片，剩下的不足5也要有一个切片，但是在我们现在的
                  // 情况0.4也要设置一个切片，很浪费资源，或者会造成数据倾斜，所以我们现在就设定一个值
                  // 1.1 只要文件大小在splitSize * 1.1 之内就将其放到一个切片内 在我们当前假设下
                  // 也就是5.5 只要文件大小在5.5以内就放入到一个切片之内。
                  while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                    //我们最大的文件过来 文件大小822940 splitSize 411504
                    // 此时做除法运算明显要大于1.1
                    // 他就会将文件进行循环切割,直到比列小于1.1,每次切割 splitSize长度放到一个切片中
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                        length-bytesRemaining, splitSize, clusterMap);
                    splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                        splitHosts[0], splitHosts[1]));
                    bytesRemaining -= splitSize;
                  }
        					// 第一个文件进来 长度23 bytesRemaining 23
                  if (bytesRemaining != 0) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                        - bytesRemaining, bytesRemaining, clusterMap);
                    // 将所有文件读取到一个切片中 同理 第二个 第三个 都会分别读到一个切片中
                    splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                        splitHosts[0], splitHosts[1]));
                  }
                } else {
                  String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
                  splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
                }
              } else { 
                //Create empty hosts array for zero length files
                splits.add(makeSplit(path, 0, length, new String[0]));
              }
            }
           
        ```

      - ```scala
        // goalSize 411504 minsize为1  blockSize为32MB
        protected long computeSplitSize(long goalSize, long minSize,
                                               long blockSize) {
          //我们可以得出splitSize最终为goalSize
            return Math.max(minSize, Math.min(goalSize, blockSize));
          }
        ```

      4. 到现在我们就可以从`getSplits`方法中出来了，我们此时处于的位置是在``getPartitions``这个方法中的`allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)`这行。此时textFile() 的主要流程已经结束。将程序放行我们查看out目录，就会发现有5个分区。查看分区数据，我们就会发现word4.txt被切割成了俩个文件。

         ```java
         part-00000
         part-00001
         part-00002
         part-00003
         part-00004
         ```

      > 根据上面，我们可发现一个大致的规律当我们要读取文件之间的差距较小时一个文件对应一个分区，
      >
      > 当读取文件的差距较大时，spark会根据最小分区数将大文件进行切割。

