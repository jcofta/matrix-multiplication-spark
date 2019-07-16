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

val result = sc.textFile("data/mm_results.txt").map(line => {
  val t = line.split("\\s+");
  ((t(0).trim.toInt, t(1).trim.toInt), t(2).trim.toInt)
}).
sortByKey()

my_result.join(results).
filter( { case (k, (my_result, result)) => 
my_result == result
}).count
