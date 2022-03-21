from testdata import TestData
from dailyaggregator import DailyAggregator


def main(test_data=TestData(rand_seed=1)):
    daily_aggregator = DailyAggregator(test_data)
    # Ready for next steps processed Data in variable aggregated_data
    aggregated_data = daily_aggregator.harvest()
    aggregated_data.show()


if __name__ == '__main__':
    main()
