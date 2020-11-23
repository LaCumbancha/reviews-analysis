package rabbitmq

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = 3*funbizSigs
	signalsMap[WEEKDAYS] = 3*weekdaysSigs
	signalsMap[HASHES] = 3*hashesSigs
	signalsMap[USERS] = 3*usersSigs
	signalsMap[STARS] = 3*starsSigs

	return signalsMap
}
