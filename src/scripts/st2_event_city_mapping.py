#для тестирования в условиях ограниченных ресурсов используется выборка на 1й месяц 
from pyspark.sql.types import FloatType

import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime 


def sqr(a):
    return a*a

def find_home_city(cities):
    prev_city = ""
    count = 1
    for cur_city in cities:
        if prev_city == cur_city:
            count = count + 1
        else:
            count = 1
            
        prev_city = cur_city
        
        if count == 3:
            return cur_city
    return ""

def count_city(cities):
    prev_city = ""
    count = 0
    for cur_city in cities:
        if prev_city != cur_city:
            count = count + 1
            
    return count

def travel_array(cities):
    prev_city = ""
    cityLine = []
    for cur_city in cities:
        if prev_city != cur_city:
            cityLine.append(cur_city)
            
    return cityLine

def main():
        dt = sys.argv[1]
        events_base_path = sys.argv[2]
        output_base_path = sys.argv[3]

        conf = SparkConf().setAppName(f"CityMappingJob-{dt}")
        sc = SparkContext(conf=conf)
        spark = SQLContext(sc)

        df_city = spark.read.parquet('{events_base_path}/city_parq/city.parquet')
        df = spark.read.parquet('{events_base_path}/events_m') \
                                .filter(F.col("event_type") == "message") \
        
        df = df.crossJoin(df_city.withColumnRenamed('lat', 'city_lat').withColumnRenamed('lng', 'city_lon'))

        df = df.withColumn("rng", F.lit(2*6371) * F.asin(F.sqrt( \
                                        sqr(F.sin((F.radians('city_lat') - F.radians('lat'))/F.lit(2)) + \
                                            F.cos(F.radians('lat'))*F.cos(F.radians('city_lat'))* \
                                            sqr(F.sin((F.radians('city_lon') - F.radians('lon'))/F.lit(2)) \
                                            ))))) 

        windowCitiesRank = Window.partitionBy("event.message_id").orderBy(F.col("rng").asc())

        df_with_cities = df \
                        .withColumn("rank", F.row_number().over(windowCitiesRank)) \
                        .filter(F.col("rank") == 1) \
                        .selectExpr("event.message_from as user_id", "date", "event.message_id as message_id", "city", "event.message_ts as message_ts") \
                        .orderBy("user_id", "date")

        #df_with_cities.write.parquet('/user/nutslyc/data/tmp/messages_cities')

        #актуальный город пользователя
        windowActCity = Window.partitionBy("user_id").orderBy(F.col("date").desc())

        df_users_act_city = df_with_cities \
                            .withColumn("rank_d", F.row_number().over(windowActCity)) \
                            .filter(F.col("rank_d") == 1) \
                            .selectExpr(["user_id", "city as act_city"])
        #df_users_act_city.write.parquet('/user/nutslyc/data/tmp/df_users_act_city_m_2')


        #домашний город пользователя. Если в один день пользователь отправил сообщение из нескольких городов, то считаем это прерыванием цепочки
        #т.к. по условию задачи пользователь не должен непрерывно находиться в одном и том же городе
        #для теста выбрано значение count == 3, т.к. использованы частичные данные. Для продуктивного решения необходимо заменить значение на 27
        df_user_cities_group_dates = df_with_cities \
                            .groupBy("user_id", "date", "city") \
                            .count() \
                            .orderBy(F.col("user_id"), F.col("date").desc())

        df_user_cities_group = df_user_cities_group_dates \
                                .drop("count")

            
        find_home_city_udf = F.udf(lambda x:find_home_city(x))

        df_user_home_city = df_city_list \
                .withColumn("home_city", F.lit(find_home_city_udf("collect_list(city)"))) \
                .select("user_id", "home_city")

        #df_user_home_city.write.parquet('/user/nutslyc/data/tmp/df_users_home_city_m')

        #объединяем актуальный и домашний города для пользователей
        df_users_act_city = spark.read.parquet('/user/nutslyc/data/tmp/df_users_act_city_m_2')
        df_user_home_city = spark.read.parquet('/user/nutslyc/data/tmp/df_users_home_city_m')

        df_user_cities = df_users_act_city.join(df_user_home_city, ['user_id'], "full")

        #df_user_cities.show()

        #количество посещенных городов
        df_user_cities_group_dates = df_with_cities \
                            .groupBy("user_id", "date", "city") \
                            .count() \
                            .orderBy(F.col("user_id"), F.col("date").desc())

        df_user_cities_group = df_user_cities_group_dates \
                                .drop("count", "message_id")

        df_city_list = df_user_cities_group \
                    .orderBy(F.col("user_id"), F.col("date").desc()) \
                    .groupBy("user_id") \
                    .agg(F.collect_list("city"))
            
        count_city_udf = F.udf(lambda x:count_city(x))

        df_user_count_city = df_city_list \
                .withColumn("travel_count", F.lit(count_city_udf("collect_list(city)"))) \
                .select("user_id", "travel_count")

        #df_user_count_city.show(100)

        #объединяем с домашними городами

        df_user_cities_travel = df_user_cities.join(df_user_count_city, ['user_id'], "full")

        #список городов в порядке посещения
        df_user_cities_group_dates = df_with_cities \
                            .groupBy("user_id", "date", "city") \
                            .count() \
                            .orderBy(F.col("user_id"), F.col("date").desc())

        df_user_cities_group = df_user_cities_group_dates \
                                .drop("count", "message_id")
            
        travel_array_udf = F.udf(lambda x:travel_array(x))

        df_user_travel_array = df_city_list \
                .withColumn("travel_array", F.lit(travel_array_udf("collect_list(city)"))) \
                .select("user_id", "travel_array")

        #формируем общий массив

        df_user_cities_travel_all = df_user_cities_travel.join(df_user_travel_array, ['user_id'], "full")

        #рассчитываем время последнего события. В связи с тем, что не для всех имеющихся городов есть таймзона, на некоторых вызывается ошибка использована единая таймзона Australia/Sydney
        #Необходимо включить в требования к доработке пайплайна данных по городам обязательно указывать таймзону города

        windowLastMess = Window.partitionBy("user_id").orderBy(F.col("message_ts").desc())

        df_users_last_timestamp = df_with_cities \
                            .withColumn("rank_ts", F.row_number().over(windowLastMess)) \
                            .filter(F.col("rank_ts") == 1) \
                            .selectExpr(["user_id", "message_ts as TIME_UTC"])

        df_users_last_timestamp = df_users_last_timestamp \
                            .withColumn("local_time", F.from_utc_timestamp(F.col("TIME_UTC"),'Australia/Sydney')) \
                            .select("user_id", "local_time")

        #формируем общую витрину

        df_users = df_user_cities_travel_all.join(df_users_last_timestamp, ['user_id'], "full")
        df_users.write.parquet('f"{output_base_path}/users={dt}"')


if __name__ == "__main__":
    main()