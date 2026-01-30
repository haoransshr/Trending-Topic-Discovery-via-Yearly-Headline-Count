Trending Topic Discovery via Yearly Headline Count

Detect and track high-impact topic terms per year from ABC News headlines using Spark RDD and Spark DataFrame APIs (two standalone solutions, no pandas/numpy, no SQL strings).

⸻
TL;DR

	•	Pick top-n stopwords by global raw TF (ties → alphabetical).
	
	•	Remove stopwords, compute DF (unique per headline), pick top-m topics (ties → alphabetical).
	
	•	For each year, count headlines per topic (unique per headline), output top-k topic–count pairs sorted by count↓ then token↑.

⸻

Project Structure
.

├─ project2_rdd.py      # RDD-only implementation

├─ project2_df.py       # DataFrame-only implementation (no spark.sql)

├─ abcnews.txt          # Sample input (each line: YYYYMMDD,token token ...)

└─ README.md

⸻

Requirements

	•	Apache Spark 3.x (tested locally; cluster OK)
	
	•	Java 8+
	
	•	Python 3.7+

	•	No external libs such as numpy/pandas (per assignment rules)

⸻

Input Format

Each line is:

YYYYMMDD,term1 term2 term3 ...

	•	The year is extracted from the first 4 chars of the date.
	
	•	Terms are whitespace-separated lowercase tokens.

⸻

Output Format

For each year (ascending), print one header line and then up to k topic lines:

 year:<#topics_covered_in_that_year>

 topic\t count

 topic\t count

...

⸻


Example (n=1, m=5, k=3):

2003:2

council    3

insurance  2


2004:2

cowboys    2

eels       2


2020:1

coronavirus 3

⸻

How to Run

Important: Use the file:/// prefix for local files; Spark otherwise assumes HDFS.

⸻

RDD solution

spark-submit project2_rdd.py \"file:///home/abcnews.txt" \"file:///home/output_rdd" \1 5 3

Args:

input, output, n, m, k = top-n stopwords, top-m topics, top-k per year.

⸻

DataFrame solution

spark-submit project2_df.py \"file:///home/abcnews.txt" \"file:///home/output_df" \1 5 3

⸻

Output files

	•	RDD writer typically produces part-00000.
	
	•	DataFrame writer may produce part-00000-<UUID>-c000. This is normal; graders read file contents, not exact filenames.
	
	•	Both solutions use coalesce(1) to produce a single part file per run.

⸻

Method (What the code does)

1) Stopword filtering (global raw TF)
   
	•	Explode all tokens → (token, 1) → reduceByKey / groupBy().count().

	•	Pick top-n by count desc, ties by token asc.

	•	RDD: takeOrdered(n, key=lambda kv: (-kv[1], kv[0])) → broadcast set.

	•	DF: orderBy(desc("count"), "token").limit(n) → collect → array(*[lit(w)]).

2) Topic discovery (document frequency)
   
	•	Remove stopwords at headline granularity.

	•	Count document frequency (DF): a token counts at most once per headline.

	•	RDD: turn each headline’s tokens into a set() before counting.

	•	DF: array_distinct(clean) before explode.

	•	Pick top-m topics by DF desc, ties by token asc; broadcast (RDD) or constant array column (DF).

3) Per-year counts & ranking
   
	•	For each headline, take distinct tokens ∩ topic set.

	•	Count (year, topic) -> #headlines.

	•	Rank within year: count desc, then token asc. Keep top-k.

	•	RDD: sort list then slice.

	•	DF: Window.partitionBy("year").orderBy(desc("count"), "token") + row_number().

4) Output formatting
   
	•	For each year, print header year:total_topics_in_that_year then up to k topic lines with \t between token and count.

⸻

Why this is efficient

	•	Broadcast small sets (stopwords/topics) to executors (RDD); constant arrays in DF.
	
	•	takeOrdered avoids collecting full vocab—returns only top-n/m.
	
	•	Avoid groupByKey in RDD final aggregation: use aggregateByKey to reduce shuffle pressure.
	
	•	DF high-order array functions: array_except, array_intersect, array_distinct keep the entire solution in pure DataFrame API (no spark.sql).
	
	•	Window functions are part of the DataFrame API (allowed) and are the cleanest way to get per-group top-k.

⸻

Example (tiny)

Input:
20030219,council chief executive council
20030219,council welcomes insurance
20040501,cowboys eels cowboys

Command:
spark-submit project2_df.py file:///.../abcnews.txt file:///.../out 1 3 2

Possible output:
2003:2
chief   1
executive       1
2004:1
eels    1
