package rabbitmq

type FunnyBusinessData struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	Funny 			int 						`json:"funny",omitempty`
}

type CityBusinessData struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	City 			string 						`json:"city",omitempty`
}

type FunnyCityData struct {
	City 			string 						`json:"city",omitempty`
	Funny 			int 						`json:"funny",omitempty`
}
