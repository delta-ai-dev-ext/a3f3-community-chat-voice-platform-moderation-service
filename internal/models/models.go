// Package models holds the domain types, request/response shapes, pagination
// helpers, and the sentinel domain errors that handlers map to HTTP statuses.
package models

import "time"

// SanctionType enumerates the moderation actions this service owns.
type SanctionType string

// Supported sanction types.
const (
	SanctionTypeMute SanctionType = "mute"
	SanctionTypeKick SanctionType = "kick"
	SanctionTypeBan  SanctionType = "ban"
)

// Valid reports whether the type is one of the three sanctioned actions.
func (t SanctionType) Valid() bool {
	switch t {
	case SanctionTypeMute, SanctionTypeKick, SanctionTypeBan:
		return true
	default:
		return false
	}
}

// Permission returns the API-001 permission that authorizes this sanction type.
func (t SanctionType) Permission() Permission {
	switch t {
	case SanctionTypeMute:
		return PermissionMuteMembers
	case SanctionTypeKick:
		return PermissionKickMembers
	case SanctionTypeBan:
		return PermissionBanMembers
	default:
		return ""
	}
}

// LiftedAction returns the event action reported when the sanction is lifted.
// Kicks have no lift action because they are instantaneous.
func (t SanctionType) LiftedAction() string {
	switch t {
	case SanctionTypeMute:
		return "unmute"
	case SanctionTypeBan:
		return "unban"
	default:
		return ""
	}
}

// SanctionSource records why a sanction or a lift came into existence.
type SanctionSource string

// Supported sanction sources.
const (
	SourceModerator SanctionSource = "moderator"
	SourceExpiry    SanctionSource = "expiry"
	SourceAppeal    SanctionSource = "appeal"
)

// SanctionStatus is the API-facing lifecycle state of a sanction.
type SanctionStatus string

// Supported sanction statuses. Expired is a computed view for a timed mute that
// has passed its expiry but has not been swept yet.
const (
	StatusActive  SanctionStatus = "active"
	StatusLifted  SanctionStatus = "lifted"
	StatusExpired SanctionStatus = "expired"
)

// Valid reports whether the status is a known read filter value.
func (s SanctionStatus) Valid() bool {
	switch s {
	case StatusActive, StatusLifted, StatusExpired:
		return true
	default:
		return false
	}
}

// AppealStatus is the lifecycle state of a member appeal.
type AppealStatus string

// Supported appeal statuses.
const (
	AppealStatusSubmitted  AppealStatus = "submitted"
	AppealStatusUpheld     AppealStatus = "upheld"
	AppealStatusOverturned AppealStatus = "overturned"
)

// Valid reports whether the status is a known read filter value.
func (s AppealStatus) Valid() bool {
	switch s {
	case AppealStatusSubmitted, AppealStatusUpheld, AppealStatusOverturned:
		return true
	default:
		return false
	}
}

// Decision is the moderator verdict on an appeal.
type Decision string

// Supported appeal decisions.
const (
	DecisionUpheld     Decision = "upheld"
	DecisionOverturned Decision = "overturned"
)

// Valid reports whether the decision is one of the two allowed verdicts.
func (d Decision) Valid() bool {
	return d == DecisionUpheld || d == DecisionOverturned
}

// Status maps a decision onto the resulting appeal status.
func (d Decision) Status() AppealStatus { return AppealStatus(d) }

// AuditAction enumerates the append-only audit log action types.
type AuditAction string

// Supported audit actions; the set is fixed by the schema check constraint.
const (
	AuditActionSanctionIssued       AuditAction = "sanction_issued"
	AuditActionSanctionLifted       AuditAction = "sanction_lifted"
	AuditActionSanctionExpired      AuditAction = "sanction_expired"
	AuditActionAppealDecided        AuditAction = "appeal_decided"
	AuditActionAutomodConfigChanged AuditAction = "automod_config_changed"
)

// Valid reports whether the action is a known audit action.
func (a AuditAction) Valid() bool {
	switch a {
	case AuditActionSanctionIssued, AuditActionSanctionLifted, AuditActionSanctionExpired,
		AuditActionAppealDecided, AuditActionAutomodConfigChanged:
		return true
	default:
		return false
	}
}

// Permission is a community-level permission understood by API-001.
type Permission string

// Permissions this service requests from community_service (API-001).
const (
	PermissionMuteMembers   Permission = "mute_members"
	PermissionKickMembers   Permission = "kick_members"
	PermissionBanMembers    Permission = "ban_members"
	PermissionManageAutomod Permission = "manage_automod"
	PermissionViewAuditLog  Permission = "view_audit_log"
	PermissionReviewAppeals Permission = "review_appeals"
)

// Sanction is the authority for every moderation decision.
type Sanction struct {
	ID           string          `json:"id"`
	CommunityID  string          `json:"community_id"`
	TargetUserID string          `json:"target_user_id"`
	ActorUserID  *string         `json:"actor_user_id"`
	Type         SanctionType    `json:"type"`
	Reason       *string         `json:"reason"`
	Source       SanctionSource  `json:"source"`
	IssuedAt     time.Time       `json:"issued_at"`
	ExpiresAt    *time.Time      `json:"expires_at"`
	LiftedAt     *time.Time      `json:"lifted_at"`
	LiftSource   *SanctionSource `json:"lift_source"`
	LiftReason   *string         `json:"lift_reason"`
	Status       SanctionStatus  `json:"status"`
}

// ComputeStatus derives the API-facing status from the stored timestamps. A
// timed mute is "expired" only until the sweep lifts it; afterwards it is
// "lifted" with lift_source expiry.
func (s *Sanction) ComputeStatus(now time.Time) SanctionStatus {
	switch {
	case s.LiftedAt != nil:
		return StatusLifted
	case s.ExpiresAt != nil && !s.ExpiresAt.After(now):
		return StatusExpired
	default:
		return StatusActive
	}
}

// Active reports whether the sanction currently restricts the member.
func (s *Sanction) Active(now time.Time) bool { return s.ComputeStatus(now) == StatusActive }

// Appealable reports whether the member may appeal this sanction: only active
// mutes and bans are appealable, kicks never are.
func (s *Sanction) Appealable(now time.Time) bool {
	if !s.Active(now) {
		return false
	}
	return s.Type == SanctionTypeMute || s.Type == SanctionTypeBan
}

// Appeal is a member's request to review a sanction.
type Appeal struct {
	ID              string       `json:"id"`
	SanctionID      string       `json:"sanction_id"`
	CommunityID     string       `json:"community_id"`
	AppellantUserID string       `json:"appellant_user_id"`
	Statement       string       `json:"statement"`
	Status          AppealStatus `json:"status"`
	ReviewerUserID  *string      `json:"reviewer_user_id"`
	DecisionNote    *string      `json:"decision_note"`
	CreatedAt       time.Time    `json:"created_at"`
	DecidedAt       *time.Time   `json:"decided_at"`
}

// AutomodAction is what the automatic filter does with a match.
type AutomodAction string

// Supported automod actions.
const (
	AutomodActionBlock  AutomodAction = "block"
	AutomodActionRemove AutomodAction = "remove"
)

// AutomodMatchMode selects how banned words are compared.
type AutomodMatchMode string

// Supported banned-word match modes.
const (
	AutomodMatchWholeWord AutomodMatchMode = "whole_word"
	AutomodMatchSubstring AutomodMatchMode = "substring"
)

// AutomodSpamRules is the spam-rate half of a community rule set.
type AutomodSpamRules struct {
	Enabled               bool          `json:"enabled"`
	Action                AutomodAction `json:"action"`
	MaxMessagesPerWindow  int           `json:"max_messages_per_window"`
	WindowSeconds         int           `json:"window_seconds"`
	MaxDuplicateMessages  int           `json:"max_duplicate_messages"`
	MaxMentionsPerMessage int           `json:"max_mentions_per_message"`
	MaxLinksPerMessage    int           `json:"max_links_per_message"`
}

// AutomodBannedWords is the banned-word half of a community rule set.
type AutomodBannedWords struct {
	Enabled   bool             `json:"enabled"`
	Action    AutomodAction    `json:"action"`
	MatchMode AutomodMatchMode `json:"match_mode"`
	Words     []string         `json:"words"`
}

// AutomodRuleSet is one versioned, immutable snapshot of a community's rules.
type AutomodRuleSet struct {
	CommunityID    string             `json:"community_id"`
	RuleSetVersion int                `json:"rule_set_version"`
	Spam           AutomodSpamRules   `json:"spam"`
	BannedWords    AutomodBannedWords `json:"banned_words"`
	ExemptRoleIDs  []string           `json:"exempt_role_ids"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

// AutomodRuleSetInput is the replaceable rule-set body accepted by PUT.
type AutomodRuleSetInput struct {
	Spam          AutomodSpamRules   `json:"spam"`
	BannedWords   AutomodBannedWords `json:"banned_words"`
	ExemptRoleIDs []string           `json:"exempt_role_ids"`
}

// AuditLogEntry is one immutable record of a moderator action.
type AuditLogEntry struct {
	ID           string      `json:"id"`
	CommunityID  string      `json:"community_id"`
	ActorUserID  *string     `json:"actor_user_id"`
	TargetUserID *string     `json:"target_user_id"`
	Action       AuditAction `json:"action"`
	Reason       *string     `json:"reason"`
	Detail       []byte      `json:"detail"`
	CreatedAt    time.Time   `json:"created_at"`
}

// OutboxEvent is an event committed in the same transaction as its state
// change and relayed to Kafka by the dispatcher (ADR-003).
type OutboxEvent struct {
	ID          string
	Topic       string
	MessageKey  string
	Envelope    []byte
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// IdempotencyRecord is the first accepted response for an Idempotency-Key.
type IdempotencyRecord struct {
	Scope        string
	Key          string
	RequestHash  string
	ResponseCode int
	ResponseBody []byte
	CreatedAt    time.Time
}

// MySanction is the member-facing projection of an active sanction.
type MySanction struct {
	ID           string       `json:"id"`
	CommunityID  string       `json:"community_id"`
	Type         SanctionType `json:"type"`
	Reason       *string      `json:"reason"`
	IssuedAt     time.Time    `json:"issued_at"`
	ExpiresAt    *time.Time   `json:"expires_at"`
	Appealable   bool         `json:"appealable"`
	OpenAppealID *string      `json:"open_appeal_id"`
}
