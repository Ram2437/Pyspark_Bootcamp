import sys
import configparser as cp
try:
    from pyspark import SparkContext, SparkConf

    props = cp.RawConfigParser()
    props.read(r"src/main/Resources/application.properties")

    conf = SparkConf().\
        setAppName("Total Revenue per day").\
        setMaster(props.get(sys.argv[5],'executionMode'))

    sc = SparkContext(conf=conf)
    # Total 5 arguments: input base dir, output base dir, local dir, month, environment
    InputPath = sys.argv[1]
    OutputPath = sys.argv[2]
    month = sys.argv[3]
    # localDir = sys.argv[4]

    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

    fs = FileSystem.get(Configuration())

    if(fs.exists(path(InputPath))== False):
        print("Input path does not exists")
    else:
        if(fs.exists(path(OutputPath))):
            fs.delete(path(OutputPath), True)

        orders = InputPath + "/orders"
        orders_count = sc.accumulator(0)                #ACCUMULATOR to get the count of orders_tuples

        def getOrdersTuples(x):                         #Named function to call it in map
            orders_count.add(1)
            return (int(x.split(',')[0]), 1)

        orders_filtered = sc.textFile(orders).\
            filter(lambda order: month in order.split(',')[1]).\
            map(lambda order: getOrdersTuples(order) )
            # map(lambda order: (int(order.split(',')[0]), 1))
        # print(orders_count)

        #for orders; (order_id, 1)
        #For order_items; (order_item_order_id, (order_item_product_id, order_item_subTotal))
        #After Join; (order_id, ((order_item_product_id, order_item_subTotal), 1))
        #After Map; (order_item_product_id, order_item_subTotal)

        order_items_count = sc.accumulator(0)  # ACCUMULATORS to get the count of order_items_tuples
        def getProductIdAndRevenue(x):                         #Named function to call it in map
            order_items_count.add(1)
            return x[1][0]

        order_items = InputPath + "/order_items"
        revenue_by_productId = sc.textFile(order_items). \
            map(lambda order_items:
                (int(order_items.split(',')[1]),
                 (int(order_items.split(',')[2]), float(order_items.split(',')[4])
                  ))
                ).\
            join(orders_filtered).\
            map(getProductIdAndRevenue).\
            reduceByKey(lambda total, orderid: total + orderid)
            #reduceby key gives, (order_item_product_id, product_revenue)

        localDir = sys.argv[4]
        ProductFile = open(localDir + "/products/part-00000")
        products = ProductFile.read().splitlines()

        # Convert products into RDD by (sc.parallelize) and extract prod_id and prod_name
        # after map; we get (product_id, product_revenue)
        # after Join; (product_id, (product_name, product_revenue))
        sc.parallelize(products).\
            map(lambda product: (int(product.split(',')[0]), product.split(',')[2])).\
            join(revenue_by_productId).\
            map(lambda product: product[1][0] + "\t" + str(product[1][1])).\
            saveAsTextFile(OutputPath)

except ImportError as e:
    print("Cannot import Spark modules", e)
    sys.exit(1)


'''
/Users/ramakrishnanimmathota/Research/Bootcamp/Local_ip 
/Users/ramakrishnanimmathota/Research/Bootcamp/Local_op/Revenue_Per_Product_For_Month 
2013-08 
/Users/ramakrishnanimmathota/Research/Bootcamp/Local_ip 
dev
'''
# /Users/ramakrishnanimmathota/PycharmProjects/pyspark_bootcamp