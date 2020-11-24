package rabbitmq

// Queues
const INPUT_QUEUE_NAME = "ReviewsStreamer"
const OUTPUT_EXCHANGE_NAME = "ReviewsScatter"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// This configuration allows at max 63 partitions.
const FUNBIZ = "Funbiz-Mapper"
const WEEKDAYS = "Weekday-Mapper"
const HASHES = "Hashes-Mapper"
const USERS = "Users-Mapper"
const STARS = "Stars-Mapper"
var PARTITIONER_VALUES = []string{FUNBIZ, WEEKDAYS, HASHES, USERS, STARS}
