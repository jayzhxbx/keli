package com.keli.platform.compact

import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import com.keli.platform.compact.mongo.operator.{MongoQuery, MongoSink}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}

/**
 * 需要kafka输出的数据流是有序的。因此需要设置kafka的topic的partition分区数为1。
 */

/**
 * 输入原始数据包装样例类
 * @param device_id:设备编号
 * @param time:时间
 * @param sensor_array:设备每个传感器内码值
 */
case class SensorReading(device_id:String, time:String, sensor_array:Array[Int])

/**
 * 输出结果数据样例类
 * @param device_id:设备编号
 * @param time:时间
 * @param axis:同轴信息
 * @param label:有车/空秤状态标志.0:空秤;1:有车
 * @param sensor_error:同轴传感器的偏差值,保留两位小数
 */
case class SensorResult(device_id:String, time:String, axis:String, label:Int, sensor_error:Array[String])

object StreamJob {
    // 加载工程resources目录下default.properties文件
    private final val conf = ConfigFactory.load("default.properties")

    def main(args: Array[String]): Unit = {
        // 设置打印日志的输出级别
        Logger.getLogger("org").setLevel(Level.ERROR)

        // 1.创建一个流处理的执行环境，类似于spark的sc上下文环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 可以自己指定并行度。如果不指定，在开发环境下，默认的并行度等于cpu的核心数；生产环境下由配置文件配置。
        env.setParallelism(1)
        // 设置文件系统状态后端
        env.setStateBackend(new FsStateBackend(conf.getString("flink.state.backend")))
        /*
         * 配置checkpoint
         * env.enableCheckpointing：启用checkpoint,默认是不启用的.输入参数：checkpoint间隔，默认500ms
         * env.getCheckpointConfig.setCheckpointTimeout：执行一个checkpoint的时候，
         *      从JobManager发出一个消息，让每个source添加检查点分界线的这个时间节点开始，
         *      到所有任务都完成当前状态的保存为止，这个时间不能很长。
         *      如果超出这个时间还没完成checkpoint，不做checkpoint了。
         * env.getCheckpointConfig.setMaxConcurrentCheckpoints：最多允许同时进行的checkpoint数量。
         * env.getCheckpointConfig.setMinPauseBetweenCheckpoints：两次checkpoint之间的最小间隔时间。
         *      MinPauseBetweenCheckpoints：前一次checkpoint结束，到下一次checkpoint开始间隔的时间。
         */
        env.enableCheckpointing(Time.of(conf.getInt("flink.checkpoint.interval"),TimeUnit.SECONDS).toMilliseconds)  // 单位转换为毫秒
        env.getCheckpointConfig.setCheckpointTimeout(Time.of(conf.getInt("flink.checkpoint.timeout"),TimeUnit.SECONDS).toMilliseconds)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        // 设置重启策略.固定时间间隔重启策略.输入参数(尝试重启的次数，重启时间间隔).比如说第一次重启失败了间隔多少时间后重启第二次.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))

        // 2.source接收socket文本数据流
        //val inputStream = env.socketTextStream("localhost",7777)
        // 2.source接收kafka数据流
        val properties = new Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("kafka.bootstrap.servers"))
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, conf.getString("kafka.group.id"))
        // 调试为false,实际使用需要改成true
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
        val inputStream = env.addSource(new FlinkKafkaConsumer[String](conf.getString("kafka.topic"),new SimpleStringSchema(),properties))

        // 3.transform对数据进行转换处理
        val dataStream = inputStream
            .map(new RawDataMapper())  // 输入数据格式化
            .keyBy(_.device_id)
            .map(new SensorCoaxisError(conf))  // 计算同轴偏差

        // 3.1.有车状态数据流
        val haveCarStream = dataStream.process(new FilterNotHaveCarStream()) // 过滤空秤的流数据
        // 3.2.无车状态数据流
        val notHaveCarStream = haveCarStream.getSideOutput(new OutputTag[(String,String,String)]("NotHaveCar"))

        // 4.控制台打印输出
        haveCarStream
            .map( data => {
                (data.device_id,data.time,data.sensor_error.mkString(","))
            }).print("have Car Stream")

        // 5.sink保存到mongo数据库
        haveCarStream.addSink(new MongoSink(conf))

        // 6.执行任务
        // 对于批处理的特定函数如：count、collect、print等flink会自动去execute(),不需要多此一举,这个和spark不太一样
        env.execute("stream sensor check job")
    }
}

/**
 * 读取原始数据并做mapper映射。
 * 输入：每一行数字，字符串
 * 输出：将每一行数据包装成SensorReading样例类
 */
class RawDataMapper extends MapFunction[String, SensorReading]{
    override def map(data: String): SensorReading = {
        // 每一行数据按逗号切分
        val dataArray = data.split(",")
        // 获取设备编号dev_IMEI
        val id = dataArray(0).trim
        // 获取RefreshTime
        val time = dataArray(1).trim
        // 获取label(实际应用中没有label字段)
        //val label = dataArray.last.trim.toInt
        // 计算sensor的数量(=总长度-设备编号-时间)
        val sensor_num = dataArray.length - 2

        // 获取sensor_array
        // 初始化长度为sensor_num+1(最后一位是前面所有内码的总和)的整数数组，所有元素初始化为0
        val sensor_array = new Array[Int](sensor_num+1)
        // 起始为2是因为第0位表示dev_IMEI,第1位表示RefreshTime
        // 结束2+sensor_num
        var sum = 0
        for(i <- 2 until 2+sensor_num){
            sensor_array(i-2) = dataArray(i).trim.toInt
            sum += dataArray(i).trim.toInt
        }
        sensor_array(sensor_num) = sum
        SensorReading(id,time,sensor_array)
    }
}

/**
 * 传感器同轴偏差判断
 * @param conf:配置参数
 * 输入:SensorReading类型
 * 输出:SensorResult类型
 */
class SensorCoaxisError(conf:Config) extends RichMapFunction[SensorReading,SensorResult]{
    // 使用var定义一个空的键控状态，然后在open声明周期内获取状态的句柄
    // 记录当前设备每个传感器的标定阈值.key:sensor_id,value:[mean, upper_thre, low_thre]
    var mapState:MapState[String,Array[Double]] = _
    // 记录当前设备的同轴信息
    var valueState:ValueState[String] = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)

        // 用运行时上下文获取当前状态句柄，声明一个键控状态
        mapState = getRuntimeContext.getMapState(
            new MapStateDescriptor[String,Array[Double]]("mapState",classOf[String],classOf[Array[Double]])
        )
        valueState = getRuntimeContext.getState(
            new ValueStateDescriptor[String]("valueState",classOf[String])
        )
    }

    /**
     * 在整体有车的情况下计算同轴偏差
     * @param state:两个同轴传感器的状态元组.每个元组中有三个元素:均值、上偏差、下偏差
     * @param curValue:当前同轴传感器的内码值
     * @return 同轴偏差元组
     */
    private def calCoaxisError(state: (Array[Double], Array[Double]), curValue: (Int, Int)): (Double,Double) = {
        // 同轴偏差.如果两个传感器都是空秤状态,res=(0.0,0.0);
        // 如果两个传感器都是有车状态,res=实际值;
        // 如果一个传感器是空秤状态,另一个传感器是有车状态,res=实际值.但是空秤的传感器有可能分母为0
        var res = (0.0,0.0)

        // 获取标定阈值
        val (mean_1,upper_1,lower_1) = (state._1(0),state._1(1),state._1(2))
        val (mean_2,upper_2,lower_2) = (state._2(0),state._2(1),state._2(2))

        // 如果在标定阈值范围内,处于空秤状态;如果不在标定阈值范围内,处于有车状态
        // sensor_1=0表示传感器1处于空秤状态,sensor_1=1表示传感器1处于有车状态.sensor_2同理
        var sensor_1 = 0
        var sensor_2 = 0
        if ((curValue._1 < mean_1 - lower_1) || (curValue._1 > mean_1 + upper_1)) sensor_1 += 1
        if ((curValue._2 < mean_2 - lower_2) || (curValue._2 > mean_2 + upper_2)) sensor_2 += 1

        if ( sensor_1+sensor_2==0 ) {
            // 如果两个传感器都是空秤状态,res=(0.0,0.0)
        } else if ( sensor_1+sensor_2==2 ){
            // 如果两个传感器都是有车状态,res=实际值;
            val c1 = curValue._1 - mean_1
            val c2 = curValue._2 - mean_2
            res = ((c1-c2)/c1,(c2-c1)/c2)
        } else if  ( sensor_1+sensor_2==1 ){
            // 如果一个传感器是空秤状态,另一个传感器是有车状态,res=实际值.但是空秤的传感器有可能分母为0
            var c1 = curValue._1 - mean_1
            var c2 = curValue._2 - mean_2
            if( c1==0 ) {
                // 如果sensor1是空秤状态,那么c1有可能等于0,导致分母为0
                c1 = 0.000001
            } else if ( c2==0 ){
                // 如果sensor2是空秤状态,那么c2有可能等于0,导致分母为0
                c2 = 0.000001
            }
            res = ((c1-c2)/c1,(c2-c1)/c2)
        }
        res
    }

    /**
     * 秤台整体空秤的情况
     * 1、偏差全为0.
     * 2、用当前实时内码值更新均值
     * @param in:实时内码值
     * @param coAxis:同轴信息和标定阈值
     * return : 同轴偏差
     */
    private def weightingNotHaveCar(in: SensorReading, coAxis: Array[String]):Array[String] = {
        // 创建一个数组保存偏差
        val result = new Array[String](in.sensor_array.length - 1)

        // 偏差全为0
        for( i <- result.indices ) {
            result(i) = "0.00"
        }

        // 当前内码值更新均值
        for ( axis <- coAxis ) {
            // 取出同轴的两个传感器的id
            val id_1 = axis.split("-")(0)
            val id_2 = axis.split("-")(1)
            // 根据id获取状态(id_1传感器的状态,id_2传感器的状态)
            val state = (mapState.get(id_1),mapState.get(id_2))
            // 根据id获取当前内码值(id_1传感器的当前内码值,id_2传感器的当前内码值)
            val cur = (in.sensor_array(id_1.toInt - 1),in.sensor_array(id_2.toInt - 1))
            mapState.put(id_1, Array[Double](cur._1, state._1(1), state._1(2)))
            mapState.put(id_2, Array[Double](cur._2, state._2(1), state._2(2)))
        }
        // sum值更新均值
        val sum = mapState.get("sum")
        val (sum_upper, sum_lower) = (sum(1),sum(2))
        mapState.put("sum",Array[Double](in.sensor_array.last,sum_upper,sum_lower))

        result
    }

    /**
     * 秤台整体有车的情况
     * 计算同轴偏差
     * @param in:实时内码值
     * @param coAxis:同轴信息和标定阈值
     * return : 同轴偏差
     */
    private def weightingHaveCar(in: SensorReading, coAxis: Array[String]):Array[String] = {
        // 创建一个数组保存偏差
        val result = new Array[String](in.sensor_array.length - 1)

        // 如果整体有车的情况,每一对同轴传感器可进一步分为有车和空秤
        for ( axis <- coAxis ) {
            // 取出同轴的两个传感器的id
            val id_1 = axis.split("-")(0)
            val id_2 = axis.split("-")(1)
            // 根据id获取状态(id_1传感器的状态,id_2传感器的状态)
            val state = (mapState.get(id_1),mapState.get(id_2))
            // 根据id获取当前内码值(id_1传感器的当前内码值,id_2传感器的当前内码值)
            val cur = (in.sensor_array(id_1.toInt - 1),in.sensor_array(id_2.toInt - 1))
            // 计算同轴偏差
            val res = calCoaxisError(state, cur)
            result(id_1.toInt - 1) = res._1.formatted("%.2f")
            result(id_2.toInt - 1) = res._2.formatted("%.2f")
        }
        result
    }

    /**
     * 判断空秤/有车状态，并计算同轴偏差
     *
     * @param in:输入数据
     * @return
     */
    private def errorProcess(in: SensorReading): (Int,Array[String]) = {
        // 获取同轴信息,如:1-10,2-9,3-8,4-7,5-6
        val coAxis = valueState.value().split(",")

        // 如果整体空秤的情况,则偏差全为0.同时用当前实时内码值更新均值
        // 总内码小于等于上偏差就认为空秤.因为有车状态一定会使得总内码增大,不会减小
        // mapState.get("sum") = (sum_mean, sum_upper, sum_lower)
        val sum = mapState.get("sum")
        val (sum_mean, sum_upper) = (sum(0),sum(1))
        val (label,result) = if ( in.sensor_array.last <= sum_mean + sum_upper ) {
            (0,weightingNotHaveCar(in,coAxis))
        } else { // 如果整体空车的情况，计算同轴偏差
            (1,weightingHaveCar(in,coAxis))
        }
        (label,result)
    }

    override def map(in: SensorReading) : SensorResult = {
        // 如果mapState为空,则进行mapState初始化
        if( mapState.isEmpty ){
            // 连接mongo数据库获取当前设备dev_id的标定信息
            val query = new MongoQuery(conf)
            val sensorAlign = query.queryAlign(in.device_id)
            // 获取当前设备的标定阈值
            val sensorThreshold = sensorAlign.getSensorThreshold
            // 获取当前设备的同轴信息，并在valueState中保存
            valueState.update(sensorAlign.getAxis)
            // 遍历当前设备的每一个传感器,在mapState中保存每个传感器的阈值
            for(sensor <- sensorThreshold){
                val sensor_id = sensor.getSensor_id
                val thre = Array[Double](
                    sensor.getMean, sensor.getUpper_threshold, sensor.getLower_threshold
                )
                mapState.put(sensor_id, thre)
            }

            val (label,result) = errorProcess(in)
            SensorResult(in.device_id,in.time,valueState.value(),label,result)
        } else {
            val (label,result) = errorProcess(in)
            SensorResult(in.device_id,in.time,valueState.value(),label,result)
        }
    }
}

//if( state.value() == null ) {
//    state.update(...defaultValue...);
//}

/**
 * 分流操作.
 * 1、将空秤状态的数据流分到侧输出流
 * 2、将有车状态的数据流输出到主流,并做数据存储
 */
class FilterNotHaveCarStream extends ProcessFunction[SensorResult, SensorResult]{
    override def processElement(value: SensorResult, context: ProcessFunction[SensorResult, SensorResult]#Context, collector: Collector[SensorResult]): Unit = {
        if (value.label == 1){
            // 如果当前处于有车状态，那么输出到主流
            collector.collect(value)
        } else {
            // 如果当前处于空秤状态，那么输出到侧输出流
            context.output(new OutputTag[(String,String,String)]("NotHaveCar"),
                (value.device_id,value.time,value.axis))
        }
    }
}
