package rabbitmq

import (
	"strconv"
)

func GeneratePartitionMap(partitions int) map[string]string {
	partitionsMap := make(map[string]string)

	for idx, value := range PARTITIONER_VALUES {
		partitionsMap[value] = strconv.Itoa(idx % partitions)
	}

	return partitionsMap
}
