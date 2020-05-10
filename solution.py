links = ["A: B C D", "B: A D", "C: A", "D: B C"]
deadEnds = ["A: B C D", "B: A D", "C:", "D: B C"]
spiderTraps = ["A: B C D", "B: A D", "C: C", "D: B C"]

rawTextRDD = sc.parallelize(deadEnds)  # ["A: B C D", "B: A D", "C:", "D: B C"]
initialRank = 1.0 / rawTextRDD.count()  # 0.25 = 1/4


########################### IMPLEMENTATIONS ##################################

def init_maper(raw):  # A: B C D
    rawArray = raw.split(":")  # A , B C D
    page = rawArray[0]  # A
    links = rawArray[1].strip().split(" ")  # B C D
    return page, (links, initialRank)


def enlaces_mapper(raw):  # A: B C D
    rawArray = raw.split(":")  # A , B C D
    page = rawArray[0]  # A
    links = rawArray[1].strip().split(" ")  # B C D
    return page, links


links_rdd = rawTextRDD.map(enlaces_mapper)
page_rank_RDD = rawTextRDD.map(init_maper)

def contr_mapper(row):
    links = row[1][0]  # B C D
    contribution = row[1][1] / len(links)  # number
    pagesAddedValue = []
    for link in links:
        pagesAddedValue.append((link, contribution))  # filling pagesAddedValue
    return pagesAddedValue


contributions_RDD = page_rank_RDD.flatMap(contr_mapper)

def function_suma(a, b):
    return a + b


rank_RDD = contributions_RDD.reduceByKey(function_suma)
page_rank_RDD = links_rdd.join(rank_RDD)

iter = 15
for i in range(1, iter):
    rank_RDD = page_rank_RDD.flatMap(contr_mapper).reduceByKey(function_suma)
    page_rank_RDD = links_rdd.join(rank_RDD)

sortedPageRankRDD = page_rank_RDD.sortBy(lambda pair: -pair[1][1])
sortedPageRankRDD.collect()

######################## THE SOLUTION HAS TO BE: ##############################
# [('C', ([''], 0.00240619428600049)),
#  ('B', (['A', 'D'], 0.00240619428600049)),
#  ('D', (['B', 'C'], 0.00240619428600049)),
#  ('A', (['B', 'C', 'D'], 0.001650987720184256))]
