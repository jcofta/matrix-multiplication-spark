import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Multiply
{
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Multiply matrices")
        val sc = new SparkContext(conf)

        val m = sc.textFile("data/M.txt").map(line => {
        val t = line.split("\\s+");
        (t(0).trim.toInt, t(1).trim.toInt, t(2).trim.toInt)
        })

        val n = sc.textFile("data/N.txt").map(line => {
        val t = line.split("\\s+");
        (t(0).trim.toInt, t(1).trim.toInt, t(2).trim.toInt)
        })

        val multiply = m.map(m => (m._2, m)).
            join(n.map(n => (n._1, n))).
            map( {case (k, (matrix_m, matrix_n)) => 
                ((matrix_m._1, matrix_n._2),(matrix_m._3*matrix_n._3))
            })

        val reduceValues = multiply.reduceByKey(_ + _)

        val my_result = reduceValues.sortByKey()
        my_result.collect.foreach(println)

        //Verify results
        val result = sc.textFile("data/mm_results.txt").map(line => {
        val t = line.split("\\s+");
        ((t(0).trim.toInt, t(1).trim.toInt), t(2).trim.toInt)
        }).
        sortByKey()

        val count = my_result.join(result).
        filter( { case (k, (my_result, result)) => 
        my_result == result
        }).count

        println(count)
        //396

        sc.stop()

    }
}
