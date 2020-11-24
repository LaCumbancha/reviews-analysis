package rabbitmq

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = funbizSigs
	signalsMap[WEEKDAYS] = weekdaysSigs
	signalsMap[HASHES] = hashesSigs
	signalsMap[USERS] = usersSigs
	signalsMap[STARS] = starsSigs

	return signalsMap
}
