package rabbitmq

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = RETRIES * funbizSigs
	signalsMap[WEEKDAYS] = RETRIES * weekdaysSigs
	signalsMap[HASHES] = RETRIES * hashesSigs
	signalsMap[USERS] = RETRIES * usersSigs
	signalsMap[STARS] = RETRIES * starsSigs

	return signalsMap
}
