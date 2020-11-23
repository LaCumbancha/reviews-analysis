package rabbitmq

type HashedTextData struct {
	UserId 		string 						`json:"user_id",omitempty`
	HashedText 	string 						`json:"hashed_text",omitempty`
}

type DistinctHashesData struct {
	UserId 		string 						`json:"user_id",omitempty`
	Distinct 	int 						`json:"distinct",omitempty`
}
