# PySpark 原理解析

众所周知，Spark 框架主要是由 Scala 语言实现，同时也包含少量 Java 代码。Spark 面向用户的编程接口，也是 Scala。然而，在数据科学领域，Python 一直占据比较重要的地位，仍然有大量的数据工程师在使用各类 Python 数据处理和科学计算的库，例如 numpy、Pandas、scikit-learn 等。同时，Python 语言的入门门槛也显著低于 Scala。为此，Spark 推出了 PySpark，在 Spark 框架上提供一套 Python 的接口，方便广大数据科学家使用。
本文主要从源码实现层面解析 PySpark 的实现原理，包括以下几个方面：

1. PySpark 的多进程架构
2. Python 端调用 Java、Scala 接口；
3. Python Driver端 RDD、SQL 接口；
4. Executor 端进程间通信和序列化；
5. Pandas UDF；
6. 优化方向；

## 1、PySpark 的多进程架构
PySpark 采用了 Python、JVM 进程分离的多进程架构，在 Driver、Executor 端均会同时有 Python、JVM 两个进程。当通过 spark-submit 提交一个 PySpark 的 Python 脚本时，Driver 端会直接运行这个 Python 脚本，并从 Python 中启动 JVM；而在 Python 中调用的 RDD 或者 DataFrame 的操作，会通过 Py4j 调用到 Java 的接口。在 Executor 端恰好是反过来，首先由 Driver 启动了 JVM 的 Executor 进程，然后在 JVM 中去启动 Python 的子进程，用以执行 Python 的 UDF，这其中是使用了 socket 来做进程间通信。总体的架构图如下所示：



## 2、Python Driver 如何调用 Java 的接口
上面提到，通过 spark-submit 提交 PySpark 作业后，Driver 端首先是运行用户提交的 Python 脚本，然而 Spark 提供的大多数 API 都是 Scala 或者 Java 的，那么就需要能够在 Python 中去调用 Java 接口。这里 PySpark 使用了 Py4j 这个开源库。当创建 Python 端的 SparkContext 对象时，实际会启动 JVM，并创建一个 Scala 端的 SparkContext 对象。代码实现在 python/pyspark/context.py:

```python
def _ensure_initialized(cls, instance=None, gateway=None, conf=None):
    """
    Checks whether a SparkContext is initialized or not.
    Throws error if a SparkContext is already running.
    """
    with SparkContext._lock:
        if not SparkContext._gateway:
            SparkContext._gateway = gateway or launch_gateway(conf)
            SparkContext._jvm = SparkContext._gateway.jvm
```

在 launch_gateway (python/pyspark/java_gateway.py)中，首先启动JVM 进程：

```python
SPARK_HOME = _find_spark_home()
# Launch the Py4j gateway using Spark's run command so that we pick up the
# proper classpath and settings from spark-env.sh
on_windows = platform.system() == "Windows"
script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
command = [os.path.join(SPARK_HOME, script)]
```

然后创建 JavaGateway 并 import 一些关键的 class：

```python
gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auth_token=gateway_secret,
                                             auto_convert=True))
# Import the classes used by PySpark
java_import(gateway.jvm, "org.apache.spark.SparkConf")
java_import(gateway.jvm, "org.apache.spark.api.java.*")
java_import(gateway.jvm, "org.apache.spark.api.python.*")
java_import(gateway.jvm, "org.apache.spark.ml.python.*")
java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
# TODO(davies): move into sql
java_import(gateway.jvm, "org.apache.spark.sql.*")
java_import(gateway.jvm, "org.apache.spark.sql.api.python.*")
java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
java_import(gateway.jvm, "scala.Tuple2")
```


