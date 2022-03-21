import unittest
from testdata import TestData
from dailyaggregator import DailyAggregator


class TestDailyAggregator(unittest.TestCase):
    def test_can_make_right_grouping(self):
        testing_data = TestData(rand_seed=42)
        daily_aggregator = DailyAggregator(testing_data)
        aggregated_data = daily_aggregator.harvest()
        self.assertEqual(636, aggregated_data.count())

    def test_can_make_right_counting_sms(self):
        testing_data = TestData(rand_seed=42)
        daily_aggregator = DailyAggregator(testing_data)
        aggregated_data = daily_aggregator.harvest()
        self.assertEqual(12, aggregated_data.collect()[10][2])

    def test_can_make_right_counting_traffic(self):
        testing_data = TestData(rand_seed=42)
        daily_aggregator = DailyAggregator(testing_data)
        aggregated_data = daily_aggregator.harvest()
        self.assertEqual(3, aggregated_data.collect()[10][3])

    def test_can_make_right_counting_mi(self):
        testing_data = TestData(rand_seed=42)
        daily_aggregator = DailyAggregator(testing_data)
        aggregated_data = daily_aggregator.harvest()
        self.assertEqual(0, aggregated_data.collect()[10][4])
