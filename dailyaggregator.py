import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
import datetime


class DailyAggregator:
    """Contains the functionality of the system for the process of daily data aggregation"""

    def __init__(self, data_for_aggregation):
        """Constructor.

        :param data_for_aggregation: should be an object with tarrifs_data, users_data and actions_data DataFrames.
        """
        self.data_for_aggregation = data_for_aggregation

    def harvest(self) -> pyspark.sql.dataframe.DataFrame:
        """Launch process of aggregation.

        Return completely finished aggregated PySpark DataFrame
        """
        self.__pyspark_session_initialization()
        self.__in_pyspark_dataframes_data_formatting()
        self.__column_names_renaming()
        self.__aggregate_users_actions()
        self.__column_name_formatting()

        return self.__counted_users_traffic

    def __pyspark_session_initialization(self) -> None:
        self.__spark = SparkSession.builder \
            .master("local[*]") \
            .appName("ETL_script__MP") \
            .getOrCreate()

    def __in_pyspark_dataframes_data_formatting(self) -> None:
        self.data_for_aggregation.actions_data = self.__spark.createDataFrame(self.data_for_aggregation.actions_data)

    def __column_names_renaming(self) -> None:
        newColumns = ["id", "date_label", "user_id", "serviсe_type", "volume_of_used_units"]
        self.data_for_aggregation.actions_data = self.data_for_aggregation.actions_data.toDF(*newColumns)

    def __aggregate_users_actions(self) -> None:
        self.__counted_users_traffic = self.data_for_aggregation.actions_data \
            .withColumn('calls_value', f.when((self.data_for_aggregation.actions_data.serviсe_type == 'звонок'),
                                              self.data_for_aggregation.actions_data.volume_of_used_units).otherwise(0)) \
            .withColumn('sms_value', f.when((self.data_for_aggregation.actions_data.serviсe_type == 'смс'),
                                            self.data_for_aggregation.actions_data.volume_of_used_units).otherwise(0)) \
            .withColumn('traffic_value', f.when((self.data_for_aggregation.actions_data.serviсe_type == 'трафик'),
                                                self.data_for_aggregation.actions_data.volume_of_used_units).otherwise(0)) \
            .groupBy('user_id') \
            .agg(f.sum('sms_value').alias('sum_sms'),
                 f.sum('traffic_value').alias('sum_traffic'),
                 f.sum('calls_value').alias('sum_calls'),
                 ) \
            .withColumn("date", f.lit(datetime.date.today()))

    def __column_name_formatting(self):
        self.__counted_users_traffic = self.__counted_users_traffic \
                                                .select("user_id", "date", "sum_sms", "sum_traffic", "sum_calls") \
                                                .withColumnRenamed("user_id", "Абонент") \
                                                .withColumnRenamed("date", "Дата") \
                                                .withColumnRenamed("sum_calls", "Потрачено минут") \
                                                .withColumnRenamed("sum_sms", "Потрачено смс") \
                                                .withColumnRenamed("sum_traffic", "Потрачено трафика")
