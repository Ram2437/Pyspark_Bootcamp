from pyspark import SparkConf, SparkContext
import configparser as cp                   # package to pick the data from property file
import sys

props = cp.RawConfigParser()
props.read(r"src/main/Resources/application.properties")

env = sys.argv[1]
conf = SparkConf().\
    setMaster(props.get(env,'executionMode')).\
    setAppName("Daily Revenue")

sc = SparkContext(conf=conf)
orders = sc.textFile(props.get(env, 'input.base.dir') + "/orders")
order_items = sc.textFile(props.get(env, 'input.base.dir') + "/order_items")
orders_filtered = orders.filter(lambda of: of.split(',')[-1] in ('CLOSED','COMPLETE'))
orders_filtered_map = orders_filtered.map(lambda o: (int(o.split(',')[0]), o.split(',')[1]))
order_items_map = order_items.map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[-2])))
orders_join = orders_filtered_map.join(order_items_map)
orders_join_map = orders_join.map(lambda o: o[1])
daily_revenue = orders_join_map.reduceByKey(lambda x, y: x+y)
daily_revenue_sorted = daily_revenue.sortByKey()
daily_revenue_sorted_map = daily_revenue_sorted.map(lambda o: o[0] + ',' + str(o[1]))
daily_revenue_sorted_map.saveAsTextFile(props.get(env, 'output.base.dir') + "/daily_revenue_app")