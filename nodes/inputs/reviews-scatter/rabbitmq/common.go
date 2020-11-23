package rabbitmq

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = 2 * funbizSigs
	signalsMap[WEEKDAYS] = 2 * weekdaysSigs
	signalsMap[HASHES] = 2 * hashesSigs
	signalsMap[USERS] = 2 * usersSigs
	signalsMap[STARS] = 2 * starsSigs

	return signalsMap
}
