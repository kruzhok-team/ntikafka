package ntikafka

import "go.opentelemetry.io/otel/attribute"

var (
	attrID         = attribute.Key("id")
	attrActivityID = attribute.Key("activity_id")
	attrPlayerID   = attribute.Key("player_id")
	attrContextID  = attribute.Key("context_id")
	attrEventID    = attribute.Key("event_id")
	attrPersonID   = attribute.Key("person_id")
	attrTeamID     = attribute.Key("team_id")
)

var (
	attrAchievementID     = attribute.Key("achievement_id")
	attrAchievementStatus = attribute.Key("achievement_status")
	attrAchievementRoleID = attribute.Key("achievement_role_id")
)
