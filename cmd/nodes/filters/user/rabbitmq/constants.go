package rabbitmq

// Queues
const INPUT_QUEUE_NAME = "UserAggregator"
const OUTPUT_QUEUE_NAME = "UserFilter"
const OUTPUT_EXCHANGE_NAME = "UserStarsFilter"

// This configuration allows at max 97 partitions.
var PARTITIONER_VALUES = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "_", "-", ".", "+", "Á", "À", "Â", "É", "È", "Ê", "Í", "Ì", "Î", "Ó", "Ò", "Ô", "Ú", "Ù", "Û", "á", "à", "â", "é", "è", "ê", "í", "ì", "î", "ó", "ò", "ô", "ú", "ù", "û"}
