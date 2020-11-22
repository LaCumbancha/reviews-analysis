package rabbitmq

// Queues
const INPUT_EXCHANGE_NAME = "ReviewsScatter"
const OUTPUT_EXCHANGE_NAME = "UserMapper"
const COMMON_QUEUE_NAME = "UserMapperInput"

// Topics
const INPUT_EXCHANGE_TOPIC = "Users-Mapper"

// Protocol special messages
const END_MESSAGE = "END-MESSAGE"

// This configuration allows at max 63 partitions.
var PARTITIONER_VALUES = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
