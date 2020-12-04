package properties

//// NODES OUTPUTS QUEUES AND EXCHANGES
// Funniest Cities flow.
const BusinessesScatterOutput = "BusinessesScatter"
const CitbizMapperOutput = "CityBusinessMapper"
const ReviewsScatterOutput = "ReviewsScatter"
const FunbizMapperOutput = "FunnyBusinessMapper"
const FunbizFilterOutput = "FunnyBusinessFilter"
const FunbizAggregatorOutput = "FunnyBusinessAggregator"
const FuncitJoinerOutput = "FunnyCityJoiner"
const FuncitAggregatorOutput = "FunnyCityAggregator"
const FuncitFilterOutput = "FunnyCityFilter"
const FunniestCitiesPrettierOutput = "FunniestCitiesResults"

// Weekday Histogram flow.
const WeekdayMapperOutput = "WeekdayMapper"
const WeekdayAggregatorOutput = "WeekdayAggregator"
const WeekdayHistogramPrettierOutput = "WeekdayHistogramResults"

// Bot-Users flow.
const HashMapperOutput = "HashTextMapper"
const HashAggregatorOutput = "HashTextAggregator"
const DishashAggregatorOutput = "DishashAggregator"
const BotUsersAggregatorOutput = "BotUserAggregator"
const DishashFilterOutput = "DishashFilter"
const BotUsersFilterOutput = "BotUsersFilter"
const BotUsersJoinerOutput = "BotUsersJoiner"
const BotUsersPrettierOutput = "BotUsersResults"

// Top-Users flow.
const UserMapperOutput = "UserMapper"
const UserAggregatorOutput = "UserAggregator"
const UserFilterOutput = "UserFilter"
const TopUsersPrettierOutput = "TopUsersResults"

// Best-Users flow.
const StarsMapperOutput = "StarsMapper"
const StarsFilterOutput = "StarsFilter"
const StarsAggregatorOutput = "StarsAggregator"
const BestUsersFilterOutput = "BestUsersStarsFilter"
const BestUsersJoinerOutput = "BestUsersJoiner"
const BestUsersPrettierOutput = "BestUsersResults"

//// NODES INPUTS QUEUES
// Common queues.
const FunbizMapperInput = "FunnyBusinessMapper"
const HashMapperInput = "HashMapperInput"
const StarsMapperInput = "StarsMapperInput"
const UserMapperInput = "UserMapperInput"
const WeekdayMapperInput = "WeekdayMapperInput"

//// NODES SPECIAL TOPICS
// Mappers topics.
const FunbizMapperTopic = "Funbiz-Mapper"
const HashMapperTopic = "Hashes-Mapper"
const StarsMapperTopic = "Stars-Mapper"
const UserMapperTopic = "Users-Mapper"
const WeekdayMapperTopic = "Weekday-Mapper"
