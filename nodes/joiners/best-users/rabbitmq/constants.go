package rabbitmq

// Queues
const INPUT_EXCHANGE1_NAME = "StarsAggregator"
const INPUT_EXCHANGE2_NAME = "UserStarsFilter"
const OUTPUT_QUEUE_NAME = "UserStarsJoiner"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// Consumer identifier
const CONSUMER1 = "BESTUSERJOIN1"
const CONSUMER2 = "BESTUSERJOIN2"
