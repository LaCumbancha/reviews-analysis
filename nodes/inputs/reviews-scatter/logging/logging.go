package logging

import (
	log "github.com/sirupsen/logrus"
)

func Infof(message string, bulk int) {
	if (bulk % BULK_LOG_RATE == 0) {
		log.Infof(message)
	}
}

func Debugf(message string, bulk int) {
	if (bulk % BULK_LOG_RATE == 0) {
		log.Debugf(message)
	}
}