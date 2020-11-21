package rabbitmq

// Queues
const INPUT_EXCHANGE_NAME = "ReviewsScatter"
const OUTPUT_EXCHANGE_NAME = "WeekdayAggregator"
const COMMON_QUEUE_NAME = "WeekdayMapperInput"

// Topics
const INPUT_EXCHANGE_TOPIC = "Weekday-Mapper"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// This configuration allows at max 7 partitions.
var PARTITIONER_VALUES = []string{"0", "1", "2", "3", "4", "5", "6"}
