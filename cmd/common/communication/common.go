package communication

// Protocol special messages.
const EndMessage = "END-MESSAGE"

// Retries finish attemps.
const Retries = 25

// Detect all possible end messages (could be like 'END-MESSAGE1').
func IsEndMessage(message string) bool {
	return (len(message) > 10) && (message[0:11] == EndMessage)
}
