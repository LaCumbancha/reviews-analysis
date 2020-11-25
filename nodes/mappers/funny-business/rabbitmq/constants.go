package rabbitmq

// Queues
const INPUT_EXCHANGE_NAME = "ReviewsScatter"
const OUTPUT_QUEUE_NAME = "FunnyBusinessMapper"
const COMMON_QUEUE_NAME = "FunnyBusinessMapperInput"

// Topics
const INPUT_EXCHANGE_TOPIC = "Funbiz-Mapper"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// Retries finish attemps
const RETRIES = 25
