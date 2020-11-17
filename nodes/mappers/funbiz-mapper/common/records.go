package common

type FullReview struct {
	ReviewId 		string 						`json:"review_id",omitempty`
	UserId 			string 						`json:"user_id",omitempty`
	BusinessId 		string 						`json:"business_id",omitempty`
	Stars 			int8 						`json:"stars",omitempty`
	Useful			int8 						`json:"useful",omitempty`
	Funny 			int8 						`json:"funny",omitempty`
	Cool			int8 						`json:"cool",omitempty`
	Text 			string 						`json:"text",omitempty`
	Date 			string 						`json:"date",omitempty`
}

type FunnyBusinessData struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	Funny 			int8 						`json:"funny",omitempty`
}
