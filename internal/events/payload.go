package events

import (
	"time"

	"moderation_service/internal/models"
)

// MemberSanctionChangedPayload is the EVT-004 payload. Every field is required
// by the contract; pointers are used where the schema allows null.
type MemberSanctionChangedPayload struct {
	SanctionID   string                `json:"sanction_id"`
	CommunityID  string                `json:"community_id"`
	TargetUserID string                `json:"target_user_id"`
	ActorUserID  *string               `json:"actor_user_id"`
	Action       string                `json:"action"`
	Source       models.SanctionSource `json:"source"`
	Reason       *string               `json:"reason"`
	ExpiresAt    *time.Time            `json:"expires_at"`
	Appealable   bool                  `json:"appealable"`
}

// AutomodRulesChangedPayload is the EVT-005 payload: a full rule-set snapshot
// so messaging_service can replace its projection wholesale.
type AutomodRulesChangedPayload struct {
	CommunityID    string                    `json:"community_id"`
	RuleSetVersion int                       `json:"rule_set_version"`
	Spam           models.AutomodSpamRules   `json:"spam"`
	BannedWords    models.AutomodBannedWords `json:"banned_words"`
	ExemptRoleIDs  []string                  `json:"exempt_role_ids"`
}

// AppealStatusChangedPayload is the EVT-006 payload.
type AppealStatusChangedPayload struct {
	AppealID        string              `json:"appeal_id"`
	SanctionID      string              `json:"sanction_id"`
	CommunityID     string              `json:"community_id"`
	AppellantUserID string              `json:"appellant_user_id"`
	SanctionType    models.SanctionType `json:"sanction_type"`
	Status          models.AppealStatus `json:"status"`
	ReviewerUserID  *string             `json:"reviewer_user_id"`
	DecisionNote    *string             `json:"decision_note"`
}
