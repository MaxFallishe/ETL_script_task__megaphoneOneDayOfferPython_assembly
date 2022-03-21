import unittest
from testdata import TestData


class TestTestData(unittest.TestCase):
    def test_can_create_right_shape_tariff_df_with_random_data(self):
        testing_data = TestData(rand_seed=42)
        self.assertEqual((100, 7), testing_data.tariffs_data.shape)

    def test_can_create_right_shape_users_df_with_random_data(self):
        testing_data = TestData(rand_seed=42)
        self.assertEqual((1000, 7), testing_data.users_data.shape)

    def test_can_create_right_shape_actions_df_with_random_data(self):
        testing_data = TestData(rand_seed=42)
        self.assertEqual((1000, 5), testing_data.actions_data.shape)

    def test_can_create_custom_size_tariff_df(self):
        testing_data = TestData(tariffs_to_generate=5000, rand_seed=42)
        self.assertEqual((5000, 7), testing_data.tariffs_data.shape)

    def test_can_create_custom_size_users_df(self):
        testing_data = TestData(users_to_generate=5000, rand_seed=42)
        self.assertEqual((5000, 7), testing_data.users_data.shape)

    def test_can_create_custom_size_actions_df(self):
        testing_data = TestData(actions_to_generate=5000, rand_seed=42)
        self.assertEqual((5000, 5), testing_data.actions_data.shape)
