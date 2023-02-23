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

        #Вычисляем координаты последнего местонахождения пользователя
    
        df_mess = spark.read.parquet(f'{events_base_path}/events_m') \
                                .filter(F.col("event_type") == "message") \
                                .where(f"date < {dt}") \
                                .selectExpr("event.message_from as user_id", "lat", "lon", "date")

        windowLastMess = Window.partitionBy("user_id").orderBy(F.col("date").desc())

        df_users_last_coord = df_mess \
                            .withColumn("rank_d", F.row_number().over(windowLastMess)) \
                            .filter(F.col("rank_d") == 1) \
                            .selectExpr(["user_id", "lat", "lon"])

        #Вычисляем каналы, на которые подписаны пользователи

        df_user_subs = spark.read.parquet(f'{events_base_path}}/events_m') \
                        .filter(F.col("event_type") == "subscription") \
                        .where(f"date < {dt} and subscription_channel is not null") \
                        .selectExpr("event.user as user_id", "event.subscription_channel as channel") \
                        .distinct()
                
        #Вычисляем для пользователя всех, с кем он переписывался

        df_user_pairs = spark.read.parquet(f'{events_base_path}}/events_m') \
                            .filter(F.col("event_type") == "message") \
                            .where(f"date < {dt}") 

        df_users_from_to = df_user_pairs \
                        .selectExpr("event.message_from as user_left", "event.message_to as user_right") \
                        .distinct()

        df_users_to_from = df_user_pairs \
                        .selectExpr("event.message_from as user_left", "event.message_to as user_right") \
                        .distinct()

        df_users_contacts = df_users_from_to \
                            .union(df_users_to_from) \
                            .distinct()

        #Вычисляем массив пар юзеров, подписанных на одни каналы

        df_users_channel_contacts_left = df_user_subs \
                                    .selectExpr("user_id as user_left", "channel")
        df_users_channel_contacts_right = df_user_subs \
                                    .selectExpr("user_id as user_right", "channel") 

        df_users_channel_contacts = df_users_channel_contacts_left \
                                    .join(df_users_channel_contacts_right, ["channel"], "left") \
                                    .select("user_left", "user_right") \
                                    .distinct()

        #Вычисляем пары пользователей, которые никогда не общались

        df_users_no_messages = df_users_contacts \
                                .union(df_users_channel_contacts) \
                                .groupBy("user_left", "user_right") \
                                .count() \
                                .filter(F.col("count" == 1)) \
                                .select("user_left", "user_right")

        #записываем координаты нахождения пользователей из пар

        df_users_no_messages_coord = df_users_no_messages \
                                .join(df_users_last_coord, df_users_no_messages.user_left == df_users_last_coord.user_id, "left") \
                                .drop(F.col("user_id")) \
                                .withColumnRenamed('lat', 'left_lat') \
                                .withColumnRenamed('lon', 'left_lon') \
                                .join(df_users_last_coord, df_users_no_messages.user_right == df_users_last_coord.user_id, "left") \
                                .drop(F.col("user_id")) \
                                .withColumnRenamed('lat', 'right_lat') \
                                .withColumnRenamed('lon', 'right_lon') \

        #Вычисляем расстояние между пользователями

        df_users_no_messages_distance = df_users_no_messages_coord\
                                    .withColumn("rng", F.lit(2*6371) * F.asin(F.sqrt( \
                                        F.pow(F.sin((F.radians('left_lat') - F.radians('right_lat'))/F.lit(2)),2) + \
                                            F.cos(F.radians('right_lat'))*F.cos(F.radians('left_lat'))* \
                                            F.pow(F.sin((F.radians('left_lon') - F.radians('right_lon'))/F.lit(2)),2) \
                                            )))

        #Собираем окончательные рекомендации(расстояние меньше километра)
        df_users_recomendations = df_users_no_messages_distance \
                                    .filter(F.col("rng") < 1)

        #Дополняем данными из ранее посчитанных данных о пользователях(т.к. расстояние между пользователями меньше 1 км, пренебрегаем вероятностью нахождения в разных городах
        #  и считаем город по левому пользователю).
        df_users = spark.read.parquet(f'{output_base_path}/users={dt}')

        df_users_recomendations = df_users_recomendations \
                                .join(df_users, df_users.user_id == df_users_recomendations.user_left, "left") \
                                .drop("user_id") \
                                .withColumn("processed_dttm", F.lit("dt")) \
                                .selectExpr("user_left", "user_right", "processed_dttm", "act_city as zone_id", "local_time")

        df_users_recomendations.write.parquet(f'{output_base_path}/user_recomendations={dt}')

if __name__ == "__main__":
    main()