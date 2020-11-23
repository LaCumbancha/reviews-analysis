package rabbitmq

type UserData struct {
	UserId 			string 						`json:"user_id",omitempty`
	Reviews 		int 						`json:"reviews",omitempty`
}
