from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
from pyspark.sql.window import Window

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Fill in your code here
    # 1) Load text and split into columns: year | tokens[]
        df = (spark.read.text(inputPath)
              .select(
                  substring_index("value", ",", 1).alias("date"),
                  substring_index("value", ",", -1).alias("text")
              )
              .select(
                  substring("date", 1, 4).alias("year"),
                  split("text", " ").alias("tokens")
              ))

    # 2) Global raw term-frequency  →  stopword list (size = n)
        tf = (df.select(explode("tokens").alias("token"))
                .groupBy("token").count())  # adds default column "count"
        stop_df = tf.orderBy(desc("count"), "token").limit(int(stopwords)) # dual sort: freq ↓, token ↑
        stopwords = [r["token"] for r in stop_df.collect()]      
        stop_array   = array(*[lit(w) for w in stopwords]) 

    # 3) Remove stopwords ➜ get document frequency ➜ topic list
        df_nostop = df.withColumn("clean", array_except("tokens", stop_array))

        topic_df = (df_nostop
                    .select(explode(array_distinct("clean")).alias("token"))
                    .groupBy("token").count()
                    .orderBy(desc("count"), "token")
                    .limit(int(topic)))
        topics = [r["token"] for r in topic_df.collect()]
        topics_array = array(*[lit(w) for w in topics])

    # 4) Count headlines per (year, topic) pair
        df_topics = (df_nostop.withColumn("topic_tokens", array_distinct(array_intersect("clean", topics_array)))
            .select("year", explode("topic_tokens").alias("token"))
            .groupBy("year", "token").count())

    # 5) Rank tokens inside each year, keep top-k
        win = Window.partitionBy("year").orderBy(desc("count"), "token")
        ranked = df_topics.withColumn("rn", row_number().over(win))
        topk = ranked.filter(col("rn") <= int(k))

        per_year_total = df_topics.groupBy("year").agg(count("*").alias("total"))

    # 6) Build header lines (“YYYY:T”) and body lines (“token<TAB>cnt”)
    #    then union and order
        header_lines = per_year_total.select(
            "year",
            concat_ws(":", "year", "total").alias("line"),
            lit(0).alias("order") 
        )

        body_lines = (topk.join(per_year_total, "year")
                      .select(
                          "year",
                          concat_ws("\t", "token", col("count")).alias("line"),
                          "rn"   
                      ).withColumnRenamed("rn", "order"))

        all_lines = (header_lines.unionByName(body_lines)
                     .orderBy("year", "order")
                     .select("line"))

    # 7) Save as single text file and stop Spark
        (all_lines
         .coalesce(1)
         .write.mode("overwrite").text(outputPath))       
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

