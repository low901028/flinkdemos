## 一.分类
### TunbingWindow:滚动窗口
1.前后两个计算不存在重叠
* 子主题 1
* 子主题 2
### SlidingWindow:滑动窗口
1.元素会在多个窗口中存在,存在重叠
* 子主题 1
* 子主题 2
## 二.方式
### 基于Time方式
* EventTime:
1. 每个独立event在其生产设备上产生的时间;
2.event记录的时间戳在进入flink时已经存在;
   在使用的时候需要提供时间戳提取方法
    (实现AssignTimeStampAndWaterMark接口)
3.使用eventtime时,数据决定了数据进度时间,并不受系统的物理时钟影响;
4.基于EventTime实现的程序需要指定如何生成TimeStamp和WaterMark这样能够显示event处理进度;
* IngestionTime:
1.该time记录的是event进入flink的时间;一般是在source操作时每个event记录都能得到当前source的时间,而后续的基于time的操作使用的时间戳即为该时间戳;
2.IngestTime处于EventTime和ProcessTime间;对比ProcessTime提供稳定的timestamp,成本却有点高;同时在进行每个Window操作时对应的timestamp却是相同的,不同于ProcessTime进行每个Window操作使用的不同时间戳;
对比EventTime来说面对out-of-order或late data记录时却无能为力.除此之外两者是类似的,由于IngestTime对应的timestamp是自动生成的,则watermark不需要指定;
* ProcessTime:
1.event在flink中被执行的时间,是基于当前执行机器的物理时钟(会导致不同的机器上ProcessTime存在差异)
2.执行Window的操作是基于机器物理时钟周期内达到的所有记录的操作;
(比如当应用09:15开始,对应的窗口大小1h,则第一个window[9:15, 10:00],第二个window[10:00,11:00]等等)
3.ProcessTime相对来说是一个比较简单,同时也不需要streams和machine间的协调的Window时间机制,并能保证最好的吞吐性能又保障了低延迟.
4.在分布式和异构的环境下,ProcessTime会受event到达系统的影响其确定性会出现不确定性;
### 基于Count方式
## 三.应用
### 类结构
* TimeCharacteristic
    * 目前只提供:ProcessingTime/IngestionTime/EventTime三类时间类型
* Window:
1.窗口Window主要用来将不同event分组到不同的buckets中;
2.maxTimestamp()用来标记在某一时刻,<=maxTimestamp的记录均会到达对应的Window;
3.任何实现Window抽象类的子类,需要实现equals()和hashCode()方法来保证逻辑相同的Window得到同样的处理;
4.每种Window都需要提供的Serialzer实现用于Window类型的序列化
    * TimeWindow:
1.时间类型窗口:具有一个从[start,end)间隔的窗口;
2.在使用过程中能够产生多个Window
        * maxTimestamp=end-1;
例如当前创建时间10:05,对应的窗口间隔=5min,则窗口的有效间隔[10:05, 10:10);结束点 ≈ 10:09 59:999
        * 实现equals:针对相同TimeWindow比较其窗口start和end
        * 实现hashCode: 基于start + end将long转为int
        * intersects:判断指定Window是否包含在当前窗口内
        * cover:根据指定Window和当前窗口生成新的包含两个窗口的新Window
* GlobalWindow:
1.默认使用的Window,将所有数据放置到一个窗口;对应窗口时间戳不超过Long.MAX_VALUE即可;
2.在使用过程中只会存在一个GlobalWindow;
        * maxTimestamp=Long.MAX_VALUE
        * 实现equals:只要属于相同类型即可
        * 实现hashCode: return 0;
* Serializer:
1.主要用于完成对Window序列化
2.通过继承抽象类TpyeSerializerSingleton<? extends Window>
* 接口: TypeSerializer<T>
1.描述Flink运行时处理数据类型所需的序列化和复制方法。在该接口中的方法被假定为无状态的，因此它实际上是线程安全的。
(有状态的这些方法的实现可能会导致不可预测的副作用，并且会损害程序的稳定性和正确性)
2.duplicate()
创建一个serializer的deep copy:
a.若serializer是无状态的 则直接返回this
b.若是serializer是有状态的,则需要创建该serializer的deep-copy
由于serializer可能会在多个thread中被使用,对应无状态的serializer是线程安全的,而有状态的则是存在非线程安全的风险;
3.snapshotConfiguration()
创建serializer当前配置snapshot及其关联的managed state一起存储;
配置snapshot需要包括serializer的parameter设置以及序列化格式等信息;
当一个新的serializer注册用来序列化相同的Managed State,配置snapshot需要确保新的Serializer的兼容性,也会存在状态迁移的需要;
4.ensureCompatibility()
用于完成不同的Serializer间的兼容性:
a.snapshot配置类型属于ParameterlessTypeSerializerConfig同时当前Serializer标识相同则进行兼容处理
b.当不满足a情况 则需要进行状态迁移

* 关于TimeWindow的mergeWindows:
针对TimeWindow定义的窗口集将重叠/交叉部分进行合并,减少Window的数量;
首先会将所有的Window基于其start字段进行排序,便于Window合并.
a.当前记录的Window包含迭代的Window,则会以当前Window作为key,并将迭代Window放置到Set中
b.当前记录的Window并不包含迭代的Window,重新添加一条新的记录<candidate,Set<TimeWindow>>
以下是使用伪码
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 // 指定使用eventtime
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

DataStream<UserDefinedEvent> stream = env.addSource(new FlinkKafkaConsumer09<UserDefinedEvent>(topic, schema, props));

stream
    .keyBy( (event) -> event.getUser() )
    .timeWindow(Time.hours(1)) // 指定窗口:大小=1h,以自然小时为周期
    .reduce( (a, b) -> a.add(b) )
    .addSink(...);
```
##四. Watermark
在Flink中提供了使用Eventtime来衡量event被处理额机制: Watermark.会作为DataStream的一部分进行传输并携带timestamp,比如Watermark(t)声明了达到Window数据的结束时间,换句话说也是没有DataStream中的element对应的timestamp t' <= t
