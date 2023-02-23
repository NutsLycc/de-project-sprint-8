from pyspark.sql.types import FloatType

import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime 


def main():
        dt = sys.argv[1]
        events_base_path = sys.argv[2]
        output_base_path = sys.argv[3]

        conf = SparkConf().setAppName(f"CityMappingJob-{dt}")
        sc = SparkContext(conf=conf)
        spark = SQLContext(sc)

        dt_first_month = datetime.datetime.strptime(dt, '%Y-%m-01')
        week_agg = dt.isocalendar().week

        #Определяем города событий

        df_city = spark.read.parquet(f'{events_base_path}/city_parq/city.parquet')
        df = spark.read.parquet(f'{events_base_path}/events_m') \
                    .where(f"date between {dt_first_month} and {dt}")

        df = df.crossJoin(df_city.withColumnRenamed('lat', 'city_lat').withColumnRenamed('lng', 'city_lon'))

        df = df.withColumn("rng", F.lit(2*6371) * F.asin(F.sqrt( \
                                          F.pow((F.sin((F.radians('city_lat') - F.radians('lat'))/F.lit(2)),2) + \
                                            F.cos(F.radians('lat'))*F.cos(F.radians('city_lat'))* \
                                            F.pow((F.sin((F.radians('city_lon') - F.radians('lon'))/F.lit(2)),2) \
                                              ))))) 

        windowCitiesRank = Window.partitionBy("event").orderBy(F.col("rng").asc())

        df_with_cities = df \
                        .withColumn("rank", F.row_number().over(windowCitiesRank)) \
                        .filter(F.col("rank") == 1) \
                        .selectExpr("event_type", "date", "city as zone_id") \

        #Рассчитываеи отсутствующий тип события registration по первому сообщению пользователя

        df_mess = df.filter(F.col("event_type") == "message")

        windowUserRegistration = Window.partitionBy("event.message_from").orderBy(F.col("date").asc())

        df_user_registration = df_mess \
                          .withColumn("rank", F.row_number().over(windowUserRegistration)) \
                          .filter(F.col("rank") == 1) \
                          .drop(F.col("event_type")) \
                          .withColumn("event_type", F.lit("registration")) \
                          .selectExpr("event_type", "date", "city as zone_id")


        df_events_all = df_with_cities.union(df_user_registration)

        #Рассчитываем статистики за неделю и месяц
        df_cities_month_events = df_events_all \
                                .withColumn("month", F.trunc(F.col("date"), 'month')) \
                                .groupBy("zone_id", "month") \
                                .pivot("event_type", ['message', 'reaction', 'subscription', 'registration']) \
                                .count() \
                                .withColumnRenamed('message', 'month_message') \
                                .withColumnRenamed('reaction', 'month_reaction') \
                                .withColumnRenamed('subscription', 'month_subscription') \
                                .withColumnRenamed('registration', 'month_user')
#Немного не понял комментарий по поводу исправления для недель
#Насколько я понял задачу, данные нужны только за последнюю неделю
#Возможно, я не правильно понимаю условие
#Можешь, пожалуйста, немного подробнее описать необходимое исправление для массива по неделям?
        df_events_all_week = df_events_all \
                            .withColumn("week", F.weekofyear("date")) \
                            .where(f"week = {week_agg}")

        df_cities_week_events = df_events_all_week \
                                .groupBy("zone_id", "week") \
                                .pivot("event_type", ['message', 'reaction', 'subscription', 'registration']) \
                                .count() \
                                .withColumnRenamed('message', 'week_message') \
                                .withColumnRenamed('reaction', 'week_reaction') \
                                .withColumnRenamed('subscription', 'week_subscription') \
                                .withColumnRenamed('registration', 'week_user')

        df_zones_stat = df_cities_month_events.join(df_cities_week_events, ["zone_id"], "full").na.fill(0)
        df_zones_stat.write.parquet(f'{output_base_path}/df_zones={dt}')


if __name__ == "__main__":
    main()