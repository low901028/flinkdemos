# flink简单demo应用
### 一.docker安装kafka
#### 1.下载docker镜像(如果直接下载docker镜像慢 可通过指定国内镜像仓库进行操作)
docker pull wurstmeister/zookeeper
docker pull wurstmeister/kafka

#### 2.分别运行docker镜像: zookeeper和kafka
2.1启动zookeeper
docker run -d --name zookeeper --publish 2181:2181 \
--volume /etc/localtime:/etc/localtime \
 wurstmeister/zookeeper

2.2启动kafka
docker run -d --name kafka --publish 9092:9092 \
--link zookeeper \
--env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
--env KAFKA_ADVERTISED_HOST_NAME=localhost \
--env KAFKA_ADVERTISED_PORT=9092 \
--volume /etc/localtime:/etc/localtime \
wurstmeister/kafka

#### 3.验证docker对应的容器是否启动成功
3.1 运行 docker ps，找到kafka的 CONTAINER ID，
3.2 运行 docker exec -it ${CONTAINER ID} /bin/bash，进入kafka容器。
3.3 进入kafka默认目录 /opt/kafka_2.11-0.10.1.0，
运行 bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test，
    创建一个 topic 名称为 test。

运行 bin/kafka-topics.sh --list --zookeeper zookeeper:2181 查看当前的 topic 列表。

运行一个消息生产者，指定 topic 为刚刚创建的 test ， 
     bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test，输入一些测试消息。

运行一个消息消费者，同样指定 topic 为 test， 
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning，可以接收到生产者发送的消息。
         
### 二.docker安装redis
#### 1.下载redis镜像
docker pull registry.docker-cn.com/library/redis
#### 2.启动redis镜像
docker run -d -p 6379:6379 --name myredis registry.docker-cn.com/library/redis
#### 3.查看docker ps  查看运行中的容器
#### 4.连接、查看容器,使用redis镜像执行redis-cli命令连接到刚启动的容器
sudo docker exec -it 6fb1ba029b41 redis-cli
出现类似: 127.0.0.1:6379> 

### 三.测试数据集
#### 3.1 数据集地址如下:
wget http://training.ververica.com/trainingData/nycTaxiRides.gz

wget http://training.ververica.com/trainingData/nycTaxiFares.gz
#### 3.2 数据集字段说明
```
=============================Taxi Ride数据集相关字段说明=============================
rideId         : Long      // a unique id for each ride 一次行程
taxiId         : Long      // a unique id for each taxi 本次行程使用的出租车
driverId       : Long      // a unique id for each driver 本次行程的司机
isStart        : Boolean   // TRUE for ride start events, FALSE for ride end events  行程开始标识
startTime      : DateTime  // the start time of a ride   行程开始日期
endTime        : DateTime  // the end time of a ride,    行程结束日期
                           //   "1970-01-01 00:00:00" for start events
startLon       : Float     // the longitude of the ride start location    行程开始经度
startLat       : Float     // the latitude of the ride start location     行程开始维度
endLon         : Float     // the longitude of the ride end location      行程结束经度
endLat         : Float     // the latitude of the ride end location	  行程结束维度
passengerCnt   : Short     // number of passengers on the ride		  本次行程乘客数

````
```
=============================TaxiFare数据集相关字段说明=============================
rideId         : Long      // a unique id for each ride     一次行程
taxiId         : Long      // a unique id for each taxi     本次行程的出租车
driverId       : Long      // a unique id for each driver   本次行程的司机
startTime      : DateTime  // the start time of a ride      行程开始时间
paymentType    : String    // CSH or CRD                    行程付费方式(CSH/CRD)
tip            : Float     // tip for this ride 	    本次行程的里程
tolls          : Float     // tolls for this ride           本次行程缴费
totalFare      : Float     // total fare collected          本次行程总费用
```

### 四.完整实例
```
// 读取配置参数: 
// --file-path /home/wmm/go_bench/flink_sources/nycTaxiRides.gz --output-redis 127.0.0.1 --max-delay 60 --serving-speed 600
final ParameterTool params = ParameterTool.fromArgs(args);
String path = params.get("file-path","/home/wmm/go_bench/flink_sources/nycTaxiRides.gz");
int maxDeply = params.getInt("max-delay",60);
int servingSpeed = params.getInt("serving-speed",600);

final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
env.getConfig().disableSysoutLogging();

// 指定TaxiRide
DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(path, maxDeply, servingSpeed));

DataStream<Tuple2<Long,Long>> tuples = rides.map(new MapFunction<TaxiRide, Tuple2<Long, Long>>() {
    @Override
    public Tuple2<Long, Long> map(TaxiRide ride) throws Exception {
        return new Tuple2<Long, Long>(ride.driverId, 1L); // 基于行程中的司机id划分数据 并进行统计
    }
});

KeyedStream<Tuple2<Long, Long>, Tuple> keyByDriverId = tuples.keyBy(0); // 基于司机id进行数据划分
DataStream<Tuple2<Long, Long>> rideCounts = keyByDriverId.sum(1); // 累计每个司机的里程数

RedisConfig redisConfig = new RedisConfig();
redisConfig.setHost(params.get("output-redis","127.0.0.1"));
redisConfig.setPort(6379);
redisConfig.setPassword(null);

// 直接使用匿名类实现redis sink
rideCounts.addSink(new RichSinkFunction<Tuple2<Long, Long>>() {  // 定义sink
    private transient JedisPool jedisPool;
    @Override
    public void open(Configuration parameters) throws Exception { // 新建redis pool
        try {
            super.open(parameters);
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(redisConfig.getMaxIdle());
            config.setMinIdle(redisConfig.getMinIdle());
            config.setMaxTotal(redisConfig.getMaxTotal());
            jedisPool = new JedisPool(config, redisConfig.getHost(), redisConfig.getPort(),
                    redisConfig.getConnectionTimeout(), redisConfig.getPassword(), redisConfig.getDatabase());
        } catch (Exception e) {
            LOGGER.error("redis sink error {}", e);
        }
    }

    @Override
    public void close() throws Exception { // 关闭redis链接
        try {
            jedisPool.close();
        } catch (Exception e) {
            LOGGER.error("redis sink error {}", e);
        }
    }

    @Override
    public void invoke(Tuple2<Long, Long> val, Context context) throws Exception { // 执行将内容落地redis
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.set(val.f0.toString(),val.f1.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != jedis){
                if (jedis != null) {
                    try {
                        jedis.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
});
//rideCounts.print();

JobExecutionResult result = env.execute("Ride Count By DriverID");
```
