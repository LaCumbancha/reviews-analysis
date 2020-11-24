package rabbitmq

// Queues
const INPUT_EXCHANGE_NAME = "ReviewsScatter"
const OUTPUT_QUEUE_NAME = "StarsMapper"
const COMMON_QUEUE_NAME = "StarsMapperInput"

// Topics
const INPUT_EXCHANGE_TOPIC = "Stars-Mapper"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// Consumer identifier
const CONSUMER = "STARSMAP"
