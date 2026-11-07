package models

import (
	"errors"
	"fmt"
	"net/http"
)

// Error is the single domain error type. Every failure crossing the service
// boundary is one of the sentinel values below, so handlers map an error to an
// HTTP status and stable snake_case code in exactly one place.
type Error struct {
	// Status is the HTTP status code the handler returns.
	Status int
	// Code is the stable machine-readable error code returned in "error".
	Code string
	// Detail is the human-readable explanation returned in "detail".
	Detail string
}

// Error implements the error interface.
func (e *Error) Error() string { return e.Code + ": " + e.Detail }

// WithDetail derives a copy of the error with a more specific detail message.
// The status and code are preserved so mapping stays centralized.
func (e *Error) WithDetail(detail string) *Error {
	clone := *e
	clone.Detail = detail
	return &clone
}

// WithDetailf derives a copy of the error with a formatted detail message.
func (e *Error) WithDetailf(format string, args ...any) *Error {
	return e.WithDetail(fmt.Sprintf(format, args...))
}

// Errors returned by the domain. Handlers map these; services return them.
var (
	ErrInvalidRequest = &Error{http.StatusBadRequest, "invalid_request", "The request body is not valid."}
	ErrInvalidCursor  = &Error{http.StatusBadRequest, "invalid_cursor", "The pagination cursor is not valid."}
	ErrUnauthorized   = &Error{http.StatusUnauthorized, "unauthorized", "A valid access token is required."}

	ErrInsufficientPermission = &Error{http.StatusForbidden, "insufficient_permission", "You do not hold the required permission."}
	ErrTargetOutranksActor    = &Error{http.StatusForbidden, "target_outranks_actor", "The target outranks you in the role hierarchy."}
	ErrTargetIsOwner          = &Error{http.StatusForbidden, "target_is_owner", "The community owner cannot be sanctioned."}
	ErrTargetIsSelf           = &Error{http.StatusForbidden, "target_is_self", "You cannot apply this action to yourself."}
	ErrNotSanctionTarget      = &Error{http.StatusForbidden, "not_sanction_target", "Only the sanctioned member can appeal this sanction."}

	ErrCommunityNotFound    = &Error{http.StatusNotFound, "community_not_found", "The community does not exist."}
	ErrSanctionNotFound     = &Error{http.StatusNotFound, "sanction_not_found", "The sanction does not exist."}
	ErrAppealNotFound       = &Error{http.StatusNotFound, "appeal_not_found", "The appeal does not exist."}
	ErrAutomodNotConfigured = &Error{http.StatusNotFound, "automod_not_configured", "No rule set is configured for this community yet."}

	ErrKickNotLiftable      = &Error{http.StatusConflict, "kick_not_liftable", "Kicks cannot be lifted."}
	ErrSanctionNotActive    = &Error{http.StatusConflict, "sanction_not_active", "The sanction is no longer active."}
	ErrAppealAlreadyOpen    = &Error{http.StatusConflict, "appeal_already_open", "An appeal for this sanction is already open."}
	ErrAppealAlreadyDecided = &Error{http.StatusConflict, "appeal_already_decided", "The appeal has already been decided."}

	ErrKickNoDuration        = &Error{http.StatusUnprocessableEntity, "kick_no_duration", "duration_seconds applies to mutes only."}
	ErrBanNoDuration         = &Error{http.StatusUnprocessableEntity, "ban_no_duration", "duration_seconds applies to mutes only."}
	ErrSanctionNotAppealable = &Error{http.StatusUnprocessableEntity, "sanction_not_appealable", "Only mutes and bans can be appealed."}
	ErrStatementTooLong      = &Error{http.StatusUnprocessableEntity, "statement_too_long", "The appeal statement exceeds 2000 characters."}
	ErrDecisionNoteTooLong   = &Error{http.StatusUnprocessableEntity, "decision_note_too_long", "The decision note exceeds 2000 characters."}
	ErrReasonTooLong         = &Error{http.StatusUnprocessableEntity, "reason_too_long", "The reason exceeds 1000 characters."}
	ErrInvalidAutomodConfig  = &Error{http.StatusUnprocessableEntity, "invalid_automod_config", "The rule set failed validation."}

	ErrAuthorizationUnavailable = &Error{http.StatusServiceUnavailable, "authorization_unavailable", "The authorization service is temporarily unavailable."}
	ErrInternal                 = &Error{http.StatusInternalServerError, "internal", "An unexpected error occurred."}
)

// DenialError maps an API-001 denial_reason onto the 403 error code the API
// returns. A denial is not a transport failure; it always produces an Error.
func DenialError(reason string) *Error {
	switch reason {
	case "target_outranks_actor":
		return ErrTargetOutranksActor
	case "target_is_owner":
		return ErrTargetIsOwner
	case "target_is_self":
		return ErrTargetIsSelf
	case "missing_permission", "":
		return ErrInsufficientPermission
	case "actor_not_member":
		return ErrInsufficientPermission.WithDetail("You are not an active member of this community.")
	default:
		return ErrInsufficientPermission
	}
}

// AsError extracts the domain error from err. Unrecognized errors are reported
// as ErrInternal so no internal detail leaks to the client.
func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	var domainErr *Error
	if errors.As(err, &domainErr) {
		return domainErr
	}
	return ErrInternal
}
