package rabbitmq

type FullBusiness struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	Name 			string 						`json:"name",omitempty`
	Address 		string 						`json:"address",omitempty`
	City 			string 						`json:"city",omitempty`
	State			string 						`json:"state",omitempty`
	PostalCode 		string 						`json:"postal_code",omitempty`
	Latitude		float32 					`json:"latitude",omitempty`
	Longitude 		float32 					`json:"longitude",omitempty`
	Stars 			float32 					`json:"stars",omitempty`
	ReviewCount		int 						`json:"review_count",omitempty`
	IsOpen			int 						`json:"is_open",omitempty`
	Attributes		map[string]string			`json:"attributes",omitempty`
	Categories		string 						`json:"categories",omitempty`
	Hours			map[string]string			`json:"hours",omitempty`
}

type CityBusinessData struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	City 			string 						`json:"city",omitempty`
}
