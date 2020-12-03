package communication

// Protocol special messages.
const END_MESSAGE = "END-MESSAGE"

// Retries finish attemps.
const RETRIES = 25

// Detect all possible end messages (could be like 'END-MESSAGE1').
func IsEndMessage(message string) bool {
	return (len(message) > 10) && (message[0:11] == END_MESSAGE)
}
