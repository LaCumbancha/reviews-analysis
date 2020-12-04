package utils

import (
	"strconv"
)

func GeneratePartitionMap(partitions int, partitionableValues []string) map[string]string {
	partitionsMap := make(map[string]string)

	for idx, value := range partitionableValues {
		partitionsMap[value] = strconv.Itoa(idx % partitions)
	}

	return partitionsMap
}
