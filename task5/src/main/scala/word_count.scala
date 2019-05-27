import org.apache.spark.{SparkConf, SparkContext}

object word_numbers {
  def main(args: Array[String]): Unit =
  {
    val spark_conf = new SparkConf().setAppName("word_amount").setMaster("local")
    val spark_cont = new SparkContext(spark_conf)

    //Читаем тексты
    val text_1 = spark_cont.textFile("C:\\Users\\Dmitry\\PycharmProjects\\ml_tasks\\task5\\data\\text_1.txt")
    val text_2 = spark_cont.textFile("C:\\Users\\Dmitry\\PycharmProjects\\ml_tasks\\task5\\data\\text_2.txt")

    //Оставляем только буквы
    val regex = """[^a-zA-Z ]""".r

    //Разобьем тексты на слова, переведем все символы в нижний регистр
    val text_1_words = text_1.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
    val text_2_words = text_2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" ")).collect()

    //Считаем количество вхождений каждого слова для text_1
    val text_1_words_count = text_1_words.map(word=>(word, 1)).reduceByKey(_+_).collect()

    //Считаем количество вхождений каждого слова для text_1, который есть в text_2
    val words_text_1_from_text_2 = text_1_words.filter(word=>text_2_words.contains(word))
      .map(word=>(word, 1)).reduceByKey(_+_).collect()

    println("word_amount text_1:")
    text_1_words_count.foreach(println)
    println("word_amount text_1 that are in text_2:")
    words_text_1_from_text_2.foreach(println)
  }
}