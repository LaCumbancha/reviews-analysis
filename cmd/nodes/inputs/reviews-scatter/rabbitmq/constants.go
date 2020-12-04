package rabbitmq

// Queues
const OUTPUT_EXCHANGE_NAME = "ReviewsScatter"

// Topics
const FUNBIZ = "Funbiz-Mapper"
const WEEKDAYS = "Weekday-Mapper"
const HASHES = "Hashes-Mapper"
const USERS = "Users-Mapper"
const STARS = "Stars-Mapper"
var PARTITIONER_VALUES = []string{FUNBIZ, WEEKDAYS, HASHES, USERS, STARS}
