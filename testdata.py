import pandas as pd
from faker import Faker
import datetime
import random


# Not forget to add random seed attribute
class TestData:
    """Implements random test data generation functions for the daily aggregation script.

    Data generation is performed at the moment of the present date and time.
    """

    __fake = Faker('ru_RU')

    def __init__(self, tariffs_to_generate=100, users_to_generate=1000, actions_to_generate=1000, rand_seed=None):
        """Constructor.

        :param tariffs_to_generate: positive value of type int defining the number of generated rows of tariff dataframe.
        :param users_to_generate: positive value of type int defining the number of generated rows of users dataframe.
        :param actions_to_generate: positive value of type int defining the number of generated rows of actions dataframeю.
        :param rand_seed: optional value that determines the seed of all random generations in the class
        """

        self.tariff_number_of_rows_to_generate = tariffs_to_generate
        self.users_number_of_rows_to_generate = users_to_generate
        self.actions_number_of_rows_to_generate = actions_to_generate
        self.rand_seed = rand_seed

        self.__fake.seed_instance(self.rand_seed)
        random.seed(self.rand_seed)

        self.tariffs_data = self.__generate_tariff_df()
        self.users_data = self.__generate_users_df()
        self.actions_data = self.__generate_actions_df()

    # Генерируем таблицу Тарифы
    def __generate_tariff_df(self) -> pd.DataFrame:
        self.__tariff_df_column_names = ["id", "Название", "Дата начала действия", "Дата конца действия", "Объем минут",
                                         "Объем смс", "Объем трафика (мб)"]

        # Генерация значений для столбца "id"
        self.__tariff_id_column_data = [i for i in range(self.tariff_number_of_rows_to_generate)]

        # Генерация значений для столбца "Название"
        self.__tariff_name_column_data = []
        for i in range(self.tariff_number_of_rows_to_generate):
            self.__random_words = self.__fake.words(nb=2)
            self.__edited_random_words = list(map(lambda x: x[0].upper() + x[1:], self.__random_words))
            self.__tariff_generated_name = "".join(self.__edited_random_words)
            self.__tariff_name_column_data.append(self.__tariff_generated_name)

        # Генерация значений для столбца "Дата начала действия"
        self.__tariff_possible_earliest_date_of_start = datetime.date(1993, 7, 17)
        self.__tariff_date_of_start_column_data = [
            self.__fake.date_between(self.__tariff_possible_earliest_date_of_start)
            for _ in range(self.tariff_number_of_rows_to_generate)]

        # Генерация значений для столбца "Дата конца действия"
        self.__tariff_possible_latest_date_of_start = datetime.date(2030, 1, 1)
        self.__tariff_date_of_end_column_data = [
            self.__fake.date_between(self.__tariff_date_of_start_column_data[i],
                                     self.__tariff_possible_latest_date_of_start)
            for i in range(self.tariff_number_of_rows_to_generate)]

        # Генерация значений для столбца "Объем минут"
        self.__tariff_min_possible_volume_of_minutes = 0
        self.__tariff_max_possible_volume_of_minutes = 1000
        self.__tariff_volume_of_minutes_column_data = [
            random.randint(self.__tariff_min_possible_volume_of_minutes, self.__tariff_max_possible_volume_of_minutes)
            for _ in range(self.tariff_number_of_rows_to_generate)]

        # Генерация значений для столбца "Объем смс"
        self.__tariff_min_possible_volume_of_sms = 0
        self.__tariff_max_possible_volume_of_sms = 1000
        self.__tariff_volume_of_sms_column_data = [
            random.randint(self.__tariff_min_possible_volume_of_sms, self.__tariff_max_possible_volume_of_sms)
            for _ in range(self.tariff_number_of_rows_to_generate)]

        # Генерация значений для столбца "Объем трафика (мб)"
        self.__tariff_min_possible_volume_of_traffic_mb = 1024
        self.__tariff_max_possible_volume_of_traffic_mb = 16384
        self.__tariff_volume_of_traffic_column_data = [
            random.randint(self.__tariff_min_possible_volume_of_traffic_mb,
                           self.__tariff_max_possible_volume_of_traffic_mb)
            for _ in range(self.tariff_number_of_rows_to_generate)]

        # Объединение сгенерированных данных в DataFrame
        self.__tariff_df_data = {self.__tariff_df_column_names[0]: self.__tariff_id_column_data,
                                 self.__tariff_df_column_names[1]: self.__tariff_name_column_data,
                                 self.__tariff_df_column_names[2]: self.__tariff_date_of_start_column_data,
                                 self.__tariff_df_column_names[3]: self.__tariff_date_of_end_column_data,
                                 self.__tariff_df_column_names[4]: self.__tariff_volume_of_minutes_column_data,
                                 self.__tariff_df_column_names[5]: self.__tariff_volume_of_sms_column_data,
                                 self.__tariff_df_column_names[6]: self.__tariff_volume_of_traffic_column_data,
                                 }

        self.__tariff_df = pd.DataFrame(self.__tariff_df_data)
        return self.__tariff_df

    # Генерируем таблицу Абоненты/Пользователи
    def __generate_users_df(self) -> pd.DataFrame:
        self.__users_df_column_names = ["id", "Текущий баланс", "Дата добавления", "Возраст", "Город проживания",
                                        "Временная метка последней активности", "Активный тариф"]

        # Генерация значений для столбца "id"
        self.__users_id_column_data = [i for i in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Текущий баланс"
        self.__users_min_possible_volume_of_balance = 0
        self.__users_max_possible_volume_of_balance = 1000
        self.__users_balance_column_data = [
            round(random.uniform(self.__users_min_possible_volume_of_balance,
                                 self.__users_max_possible_volume_of_balance),
                  2)
            for _ in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Дата добавления"
        self.__users_possible_earliest_date_of_registration = datetime.date(1993, 7, 17)
        self.__users_date_of_registration_column_data = [
            self.__fake.date_between(self.__users_possible_earliest_date_of_registration)
            for _ in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Возраст"
        self.__users_min_possible_age = 18
        self.__users_max_possible_age = 100
        self.__users_age_column_data = [random.randint(self.__users_min_possible_age, self.__users_max_possible_age)
                                        for _ in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Город проживания"
        self.__users_city_column_data = [self.__fake.city() for _ in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Временная метка последней активности"
        self.__users_possible_earliest_date_of_activity = datetime.date(1993, 7, 17)
        self.__users_last_activity_column_data = [
            self.__fake.date_time_between(self.__users_possible_earliest_date_of_activity)
            for _ in range(self.users_number_of_rows_to_generate)]

        # Генерация значений для столбца "Активный тариф"
        self.__users_active_tariff_id_column_data = [random.choice(self.__tariff_id_column_data) for _ in
                                                     range(self.users_number_of_rows_to_generate)]

        # Объединение сгенерированных данных в DataFrame
        self.__users_df_data = {self.__users_df_column_names[0]: self.__users_id_column_data,
                                self.__users_df_column_names[1]: self.__users_balance_column_data,
                                self.__users_df_column_names[2]: self.__users_date_of_registration_column_data,
                                self.__users_df_column_names[3]: self.__users_age_column_data,
                                self.__users_df_column_names[4]: self.__users_city_column_data,
                                self.__users_df_column_names[5]: self.__users_last_activity_column_data,
                                self.__users_df_column_names[6]: self.__users_active_tariff_id_column_data,
                                }

        self.__users_df = pd.DataFrame(self.__users_df_data)
        return self.__users_df

    # Генерируем таблицу События
    def __generate_actions_df(self) -> pd.DataFrame:
        self.____actions_df_column_names = ["id", "Метка времени", "id абонента", "Тип услуги (звонок, смс, трафик)",
                                            "Объем затраченных единиц"]

        # Генерация значений для столбца "id"
        self.__actions_id_column_data = [i for i in range(self.actions_number_of_rows_to_generate)]

        # Генерация значений для столбца "Метка времени"
        self.__actions_date_label_column_data = [self.__fake.date_time_between(datetime.date.today())
                                                 for _ in range(self.actions_number_of_rows_to_generate)]

        # Генерация значений для столбца "id абонента"
        self.__actions_users_id_column_data = [random.choice(self.__users_id_column_data) for _ in
                                               range(self.actions_number_of_rows_to_generate)]

        # Генерация значений для столбца "Тип услуги (звонок, смс, трафик)"
        self.__actions_possible_service_types = ["звонок", "смс", "трафик"]
        self.__actions_service_type_column_data = [random.choice(self.__actions_possible_service_types)
                                                   for _ in range(self.actions_number_of_rows_to_generate)]

        # Генерация значений для столбца "Объем затраченных единиц"
        self.__actions_min_possible_volume_of_used_units = 1
        self.__actions_max_possible_volume_of_used_units = 10
        self.__actions_volume_of_used_units = [
            random.randint(self.__actions_min_possible_volume_of_used_units,
                           self.__actions_max_possible_volume_of_used_units)
            for _ in range(self.actions_number_of_rows_to_generate)]

        # Объединение сгенерированных данных в DataFrame
        self.__actions_df_data = {self.____actions_df_column_names[0]: self.__actions_id_column_data,
                                  self.____actions_df_column_names[1]: self.__actions_date_label_column_data,
                                  self.____actions_df_column_names[2]: self.__actions_users_id_column_data,
                                  self.____actions_df_column_names[3]: self.__actions_service_type_column_data,
                                  self.____actions_df_column_names[4]: self.__actions_volume_of_used_units,
                                  }

        self.__actions_df = pd.DataFrame(self.__actions_df_data)
        return self.__actions_df
