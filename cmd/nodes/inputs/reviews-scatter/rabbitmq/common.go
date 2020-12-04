package rabbitmq

import (
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = comms.Retries * funbizSigs
	signalsMap[WEEKDAYS] = comms.Retries * weekdaysSigs
	signalsMap[HASHES] = comms.Retries * hashesSigs
	signalsMap[USERS] = comms.Retries * usersSigs
	signalsMap[STARS] = comms.Retries * starsSigs

	return signalsMap
}
