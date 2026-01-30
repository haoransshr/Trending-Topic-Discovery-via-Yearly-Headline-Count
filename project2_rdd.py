from pyspark import SparkContext, SparkConf
import sys
import math

def parse_line(line):
    """
    Split a raw line like 'YYYYMMDD,token1 token2 ...'
    into (year, [token, token, ...]).
    """
    date, text = line.split(",", 1)
    return date[:4], text.strip().split()

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        # Fill in your code here
        headlines = sc.textFile(inputPath).map(parse_line)
        # 1) Global raw term frequency  → top-n stopwords
        tf = (headlines
              .flatMap(lambda x: x[1])
              .map(lambda w: (w, 1))
              .reduceByKey(lambda a, b: a + b))
        # takeOrdered applies the dual sort: count ↓ then token ↑
        stop_set = set(tf.takeOrdered(int(stopwords), key=lambda kv: (-kv[1], kv[0])))
        stop_set = set(w for w, _ in stop_set)
        bc_stop = sc.broadcast(stop_set)


        # 2) Document frequency (DF) after removing stopwords
        #    → top-m topic tokens
        tokens_no_stop = headlines.map(
            lambda x: set([w for w in x[1] if w not in bc_stop.value])
        )
        df = (tokens_no_stop
              .flatMap(lambda s: [(w, 1) for w in s])
              .reduceByKey(lambda a, b: a + b))
        topic_list = df.takeOrdered(int(topic), key=lambda kv: (-kv[1], kv[0]))
        topic_set = set(w for w, _ in topic_list)
        bc_topic = sc.broadcast(topic_set)

        # 3) Count (year, topic) → #headlines
        year_topic = (headlines.flatMap(
            lambda x: [((x[0], w), 1) for w in set(x[1]) if w in bc_topic.value]
        ).reduceByKey(lambda a, b: a + b)) 

        # 3) Aggregate per year & keep list[(token, count)]
        by_year = (year_topic
                .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
                .aggregateByKey([],  
                    lambda acc, v: acc + [v],    
                    lambda a, b: a + b)      
                )
                
        # 5) Sort tokens inside each year, slice top-k,
        #    then flatten to requested text format                
        formatted = (by_year.mapValues(list)
                     .mapValues(lambda lst: sorted(lst, key=lambda kv: (-kv[1], kv[0])))
                     .mapValues(lambda lst: (len(lst), lst[:int(k)]))
                     .sortByKey()
                     .flatMap(lambda kv: 
                         [f"{kv[0]}:{kv[1][0]}"] +
                         [f"{t}\t{c}" for t, c in kv[1][1]]
                     ))

        formatted.coalesce(1).saveAsTextFile(outputPath)       

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])


