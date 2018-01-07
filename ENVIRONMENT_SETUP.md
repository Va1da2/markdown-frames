# DEV environment setup

In order to run entire test suite PySpark should be available on your system. Spark developers provided a `pypi` instalation of PySpark, however there are things that we need to take care before installing it.

### Java

Make sure, that you have `Java 8` on your machine. Type this into your terminal
```
java -version
```
you should see something like this
```
openjdk version "1.8.0_151"
OpenJDK Runtime Environment (build 1.8.0_151-8u151-b12-0ubuntu0.16.04.2-b12)
OpenJDK 64-Bit Server VM (build 25.151-b12, mixed mode)
```
It shouldn't matter if you use OpenJDK or OracleJDK, the important part is `1.8.x_x`.

If you do not have java JDK installed - follow one of many instructions online, for example [here](http://openjdk.java.net/install/)

### Scala

Next we need to get `Scala` (Spark is written is scala). One thing to keep in mind is that since 2.0 version Spark is based on 2.11 Scala - make sure you get the right version. The best approach is to get newest possible (or the one that you will be using on your cluster).

Download newest Scala (2.12.4) by clicking [here](https://downloads.lightbend.com/scala/2.12.4/scala-2.12.4.tgz).
Extract and move to directory convenient for you. Then in you `bash profile` set `SCALA_HOME` and add it to the path:
```
SCALA_HOME=/path/to/scala/
export PATH="$SCALA_HOME/bin:$PATH"
```
Now if you try to run a scala REPL by typing `scala` in you terminal you should see
```
Welcome to Scala 2.12.4 (OpenJDK 64-Bit Server VM, Java 1.8.0_151).
Type in expressions for evaluation. Or try :help.

scala>
```

### Spark 

After installing prerequisites we can install Spark on our machine. This follows the same pattern as scala installation.

First we need to download binaries from [here](https://spark.apache.org/downloads.html) - make sure they are for the same minor version as the `pypi` PySpark package that you will later install (as of this moment the latest Spark available on pypi is `2.2.0`).

After extracting downloaded `.tar` file and moving to your prefered location, add these lines to your bash profile.
```
SPARK_HOME=/path/to/spark/
export PATH="$SPARK_HOME/bin:$PATH"
```
Now if you type `spark-shell` you should be able to use spark scala REPL:
```
$ spark-shell
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at http://192.168.1.161:4040
Spark context available as 'sc' (master = local[*], app id = local-1514960269912).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/
         
Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_151)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

### PySpark

Final step is to create an environment for development (I use [conda](https://conda.io/docs/user-guide/tasks/manage-environments.html)) and install PySpark with [pypi](https://pypi.python.org/pypi/pyspark/2.2.0):
```
pip install pyspark
```

Now you should be able to run PySpark on your local machine.


### Jupyter notebook kernels

If you will install pypsark in new conda environment you might get into trouble for jupyter notebook having no access to that environment. Therefore if you will want to test `pyspark` in a notebook, you have to perform a manual kernel addition in you environment:
```
source activate myenv
python -m ipykernel install --user --name myenv --display-name "Python (myenv)"
```
more can be found [here](https://ipython.readthedocs.io/en/stable/install/kernel_install.html#kernels-for-different-environments)