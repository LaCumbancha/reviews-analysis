package rabbitmq

// Queues
const INPUT_EXCHANGE_NAME = "ReviewsScatter"
const OUTPUT_EXCHANGE_NAME = "WeekdayMapper"
const COMMON_QUEUE_NAME = "WeekdayMapperInput"

// Topics
const INPUT_EXCHANGE_TOPIC = "Weekday-Mapper"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// This configuration allows at max 7 partitions.
var PARTITIONER_VALUES = []string{"Sunday", "Monday", "Tuesday", "Thursday", "Friday", "Saturday", "Sunday"}
