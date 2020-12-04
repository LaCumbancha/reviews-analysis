package rabbitmq

import (
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

func GenerateSignalsMap(funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int) map[string]int {
	signalsMap := make(map[string]int)

	signalsMap[FUNBIZ] = comms.EndSignals(funbizSigs)
	signalsMap[WEEKDAYS] = comms.EndSignals(weekdaysSigs)
	signalsMap[HASHES] = comms.EndSignals(hashesSigs)
	signalsMap[USERS] = comms.EndSignals(usersSigs)
	signalsMap[STARS] = comms.EndSignals(starsSigs)

	return signalsMap
}
