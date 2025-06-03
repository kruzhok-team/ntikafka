package ntikafka

import "go.opentelemetry.io/otel/attribute"

/*
* Атрибуты пэйлоада (как ключей так и значений) сообщений.
 */

const (
	attrID                = attribute.Key("id") // Ключ для неизвестного типа идентификатора
	attrActivityID        = attribute.Key("activity_id")
	attrChallengeID       = attribute.Key("challenge_id")
	attrComplexchResultID = attribute.Key("complexch_result_id")
	attrContextID         = attribute.Key("context_id")
	attrEventID           = attribute.Key("event_id")
	attrPersonID          = attribute.Key("person_id")
	attrPlayerID          = attribute.Key("player_id")
	attrTalentID          = attribute.Key("talent_id")
	attrTeamID            = attribute.Key("team_id")
)

const (
	attrCreatedAt = attribute.Key("created_at")
	attrUpdatedAt = attribute.Key("updated_at")
	attrPassedAt  = attribute.Key("passed_at")
)

const (
	attrTalentPlayerLogID     = attribute.Key("talent_player_log_id")
	attrTalentPlayerLogAction = attribute.Key("talent_player_log_action")
)

const (
	attrAchievementID     = attribute.Key("achievement_id")
	attrAchievementStatus = attribute.Key("achievement_status")
	attrAchievementRoleID = attribute.Key("achievement_role_id")
)

const (
	attrComplexchResultPassed    = attribute.Key("complexch_result.passed")
	attrComplexchResultPassedWas = attribute.Key("complexch_result.passed_was")
	attrComplexchResultScore     = attribute.Key("complexch_result.score")
)

// Атрибуты сообщения Kafka.
const (
	attrOffset       = attribute.Key("message.offset")
	attrPartition    = attribute.Key("message.partition")
	attrKeyLength    = attribute.Key("message.key.length")
	attrValueLength  = attribute.Key("message.value.length")
	attrKeyContent   = attribute.Key("message.key.content")
	attrValueContent = attribute.Key("message.value.content")
)
