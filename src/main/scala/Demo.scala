import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
    def main(args: Array[String]): Unit = {
        //创建程序入口
        val conf: SparkConf = new SparkConf().setAppName("demo").setMaster("local[*]")
        print("1111333333333333")
        val sc = new SparkContext(conf)
        //设置日志级别
        sc.setLogLevel("WARN")

        //加载数据
        val file: RDD[String] = sc.textFile("E:\\offcn\\Spark阶段\\Spark\\SparkDay06\\资料\\data\\access.log")
        //切分
        val spliFile: RDD[Array[String]] = file.map(_.split(" "))

        //求pv
        val pv: Long = spliFile.count()
        //打印输出
        println("pv:"+pv)

        //求uv
        val ip: RDD[String] = spliFile.map(_(0))
        //去重
        val disIp: RDD[String] = ip.distinct()
        //统计uv个数
        val uv: Long = disIp.count()
        //打印输出
        println("uv:"+uv)

        //获取url
        val filFile: RDD[Array[String]] = spliFile.filter(_.length>10)
        val url: RDD[String] = filFile.map(_(10))
        //每个url记为1次
        val urlAndOne: RDD[(String, Int)] = url.map((_,1))
        //聚合
        val urlAndCount: RDD[(String, Int)] = urlAndOne.reduceByKey(_+_)
        //排序
        val sorted: RDD[(String, Int)] = urlAndCount.sortBy(_._2,false,1)
        //获取前5名
        val topFive: Array[(String, Int)] = sorted.take(5)
        //打印输出
        topFive.foreach(println)
    }

}
