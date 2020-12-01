package rabbitmq

// Queues
const OUTPUT_EXCHANGE_NAME = "ReviewsScatter"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// Topics
const FUNBIZ = "Funbiz-Mapper"
const WEEKDAYS = "Weekday-Mapper"
const HASHES = "Hashes-Mapper"
const USERS = "Users-Mapper"
const STARS = "Stars-Mapper"
var PARTITIONER_VALUES = []string{FUNBIZ, WEEKDAYS, HASHES, USERS, STARS}

// Retries finish attemps
const RETRIES = 25
