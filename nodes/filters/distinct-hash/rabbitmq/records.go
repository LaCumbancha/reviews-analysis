package rabbitmq

type DistinctHashesData struct {
	UserId 		string 						`json:"user_id",omitempty`
	Distinct 	int 						`json:"distinct",omitempty`
}
