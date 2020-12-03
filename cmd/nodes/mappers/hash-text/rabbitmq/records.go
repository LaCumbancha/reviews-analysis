package rabbitmq

type FullReview struct {
	ReviewId 		string 						`json:"review_id",omitempty`
	UserId 			string 						`json:"user_id",omitempty`
	BusinessId 		string 						`json:"business_id",omitempty`
	Stars 			int 						`json:"stars",omitempty`
	Useful			int 						`json:"useful",omitempty`
	Funny 			int 						`json:"funny",omitempty`
	Cool			int 						`json:"cool",omitempty`
	Text 			string 						`json:"text",omitempty`
	Date 			string 						`json:"date",omitempty`
}

type HashedTextData struct {
	UserId 			string 						`json:"user_id",omitempty`
	HashedText 		string 						`json:"hashed_text",omitempty`
}
