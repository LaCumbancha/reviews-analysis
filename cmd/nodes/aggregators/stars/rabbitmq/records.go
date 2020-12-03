package rabbitmq

type StarsData struct {
	UserId 		string 						`json:"user_id",omitempty`
	Stars		int 						`json:"stars",omitempty`
}

type UserData struct {
	UserId 		string 						`json:"user_id",omitempty`
	Reviews		int 						`json:"reviews",omitempty`
}
