package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/handler"
	"moderation_service/internal/models"
	"moderation_service/tests/mocks"
)

// TestAuthenticationContract: every non-probe route requires a valid token and
// answers 401 in the platform error shape (middleware contract).
func TestAuthenticationContract(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/communities/" + communityID + "/sanctions"},
		{http.MethodGet, "/communities/" + communityID + "/sanctions"},
		{http.MethodPost, "/communities/" + communityID + "/sanctions/" + sanctionID + "/lift"},
		{http.MethodPost, "/sanctions/" + sanctionID + "/appeals"},
		{http.MethodGet, "/me/sanctions"},
		{http.MethodGet, "/me/appeals"},
		{http.MethodGet, "/communities/" + communityID + "/appeals"},
		{http.MethodPost, "/communities/" + communityID + "/appeals/" + appealID + "/decision"},
		{http.MethodGet, "/communities/" + communityID + "/automod"},
		{http.MethodPut, "/communities/" + communityID + "/automod"},
		{http.MethodGet, "/communities/" + communityID + "/audit-log"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			t.Parallel()
			assertErrorBody(t, f.do(t, route.method, route.path, "", "", nil), http.StatusUnauthorized, "unauthorized")
		})
	}
}

// TestInvalidTokenIsRejected covers expired, wrong-issuer, wrong-audience, and
// unparseable tokens.
func TestInvalidTokenIsRejected(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())

	tests := []struct {
		name   string
		mutate func(jwt.MapClaims)
	}{
		{name: "expired", mutate: func(claims jwt.MapClaims) { claims["exp"] = 1 }},
		{name: "missing exp", mutate: func(claims jwt.MapClaims) { delete(claims, "exp") }},
		{name: "wrong issuer", mutate: func(claims jwt.MapClaims) { claims["iss"] = "someone_else" }},
		{name: "wrong audience", mutate: func(claims jwt.MapClaims) { claims["aud"] = []string{"someone_else"} }},
		{name: "missing subject", mutate: func(claims jwt.MapClaims) { delete(claims, "sub") }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			recorder := f.doWithClaims(t, http.MethodGet, "/me/sanctions", "", test.mutate, moderatorID, nil)
			assertErrorBody(t, recorder, http.StatusUnauthorized, "unauthorized")
		})
	}

	t.Run("garbage token", func(t *testing.T) {
		t.Parallel()
		recorder := f.doRaw(t, http.MethodGet, "/me/sanctions", "", "Bearer not-a-jwt")
		assertErrorBody(t, recorder, http.StatusUnauthorized, "unauthorized")
	})

	t.Run("missing bearer prefix", func(t *testing.T) {
		t.Parallel()
		recorder := f.doRaw(t, http.MethodGet, "/me/sanctions", "", "not-a-bearer")
		assertErrorBody(t, recorder, http.StatusUnauthorized, "unauthorized")
	})
}

// TestValidTokenPopulatesPrincipal proves the verified sub reaches the service
// layer: the created sanction records the moderator as its actor.
func TestValidTokenPopulatesPrincipal(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions",
		`{"target_user_id":"`+targetID+`","type":"ban"}`, moderatorID, nil)
	require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())

	stored := f.store.Sanctions.All()
	require.Len(t, stored, 1)
	require.NotNil(t, stored[0].ActorUserID)
	assert.Equal(t, moderatorID, *stored[0].ActorUserID)
}

// TestCreateSanctionHappyPath pins the 201 response, the trace header, and the
// write-everything-in-one-transaction behaviour.
func TestCreateSanctionHappyPath(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions",
		`{"target_user_id":"`+targetID+`","type":"ban","reason":"harassment"}`, moderatorID, nil)

	require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())
	var sanction models.Sanction
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &sanction))
	assert.Equal(t, models.SanctionTypeBan, sanction.Type)
	assert.Equal(t, models.StatusActive, sanction.Status)
	assert.Equal(t, models.SourceModerator, sanction.Source)
	assert.Equal(t, ptr(moderatorID), sanction.ActorUserID)

	assert.NotEmpty(t, recorder.Header().Get("X-Trace-Id"), "the trace id is echoed to the client")
	assert.Equal(t, 1, f.store.Sanctions.Len())
	assert.Len(t, f.store.Audit.All(), 1)
	assert.Len(t, f.store.Outbox.All(), 1)
	assert.Equal(t, 1, f.store.TxCommitCount)
}

// TestCreateSanctionFailsClosed proves US-005 end to end: an unreachable API-001
// becomes 503 authorization_unavailable with no state change.
func TestCreateSanctionFailsClosed(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, httpmock.NewStringResponder(http.StatusServiceUnavailable, `{"error":"down"}`))
	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions",
		`{"target_user_id":"`+targetID+`","type":"ban"}`, moderatorID, nil)

	assertErrorBody(t, recorder, http.StatusServiceUnavailable, "authorization_unavailable")
	assert.Zero(t, f.store.Sanctions.Len())
	assert.Empty(t, f.store.Audit.All())
	assert.Empty(t, f.store.Outbox.All())
	assert.Zero(t, f.store.TxCount)
	assert.Equal(t, 2, f.transport.GetCallCountInfo()["POST "+checkEndpoint], "one retry, then fail closed")
}

// TestCreateSanctionMapsDenials proves the denial_reason to 403 mapping.
func TestCreateSanctionMapsDenials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason string
		code   string
	}{
		{reason: "missing_permission", code: "insufficient_permission"},
		{reason: "target_outranks_actor", code: "target_outranks_actor"},
		{reason: "target_is_owner", code: "target_is_owner"},
		{reason: "target_is_self", code: "target_is_self"},
	}

	for _, test := range tests {
		t.Run(test.reason, func(t *testing.T) {
			t.Parallel()
			f := newRouterFixture(t, denyResponder(test.reason))
			recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions",
				`{"target_user_id":"`+targetID+`","type":"mute"}`, moderatorID, nil)

			assertErrorBody(t, recorder, http.StatusForbidden, test.code)
			assert.Zero(t, f.store.Sanctions.Len())
		})
	}
}

// TestCreateSanctionValidationErrors covers the 400/422 responses.
func TestCreateSanctionValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		code   string
		status int
	}{
		{
			name:   "malformed json",
			body:   `{"target_user_id":`,
			code:   "invalid_request",
			status: http.StatusBadRequest,
		},
		{
			name:   "kick with duration",
			body:   `{"target_user_id":"` + targetID + `","type":"kick","duration_seconds":60}`,
			code:   "kick_no_duration",
			status: http.StatusUnprocessableEntity,
		},
		{
			name:   "ban with duration",
			body:   `{"target_user_id":"` + targetID + `","type":"ban","duration_seconds":60}`,
			code:   "ban_no_duration",
			status: http.StatusUnprocessableEntity,
		},
		{
			name:   "unknown type",
			body:   `{"target_user_id":"` + targetID + `","type":"warn"}`,
			code:   "invalid_request",
			status: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newRouterFixture(t, allowResponder())
			recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions", test.body, moderatorID, nil)
			assertErrorBody(t, recorder, test.status, test.code)
			assert.Empty(t, f.store.Outbox.All())
		})
	}
}

// TestCreateSanctionReplaysIdempotencyKey proves the header is honoured.
func TestCreateSanctionReplaysIdempotencyKey(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	headers := map[string]string{"Idempotency-Key": "retry-1"}
	body := `{"target_user_id":"` + targetID + `","type":"mute","duration_seconds":600}`

	first := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions", body, moderatorID, headers)
	second := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions", body, moderatorID, headers)

	require.Equal(t, http.StatusCreated, first.Code)
	require.Equal(t, http.StatusCreated, second.Code)
	assert.JSONEq(t, first.Body.String(), second.Body.String())
	assert.Equal(t, 1, f.store.Sanctions.Len())
	assert.Len(t, f.store.Outbox.All(), 1)
}

// TestListSanctions covers the moderator listing, the status filter, and the
// pagination contract.
func TestListSanctions(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(activeBan())

	recorder := f.do(t, http.MethodGet, "/communities/"+communityID+"/sanctions?status=active&limit=10", "", moderatorID, nil)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var page models.SanctionPage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
	require.Len(t, page.Data, 1)
	assert.Nil(t, page.NextCursor)

	t.Run("invalid cursor", func(t *testing.T) {
		t.Parallel()
		bad := f.do(t, http.MethodGet, "/communities/"+communityID+"/sanctions?cursor=not-base64!", "", moderatorID, nil)
		assertErrorBody(t, bad, http.StatusBadRequest, "invalid_cursor")
	})

	t.Run("limit too large", func(t *testing.T) {
		t.Parallel()
		bad := f.do(t, http.MethodGet, "/communities/"+communityID+"/sanctions?limit=101", "", moderatorID, nil)
		assertErrorBody(t, bad, http.StatusBadRequest, "invalid_request")
	})

	t.Run("invalid status", func(t *testing.T) {
		t.Parallel()
		bad := f.do(t, http.MethodGet, "/communities/"+communityID+"/sanctions?status=deleted", "", moderatorID, nil)
		assertErrorBody(t, bad, http.StatusBadRequest, "invalid_request")
	})
}

// TestLiftSanction covers the success path and the 409 conflict on a repeat.
func TestLiftSanction(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(activeBan())

	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions/"+sanctionID+"/lift",
		`{"reason":"appealed"}`, moderatorID, nil)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var lifted models.Sanction
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &lifted))
	assert.Equal(t, models.StatusLifted, lifted.Status)
	require.NotNil(t, lifted.LiftSource)
	assert.Equal(t, models.SourceModerator, *lifted.LiftSource)

	second := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions/"+sanctionID+"/lift", "", moderatorID, nil)
	assertErrorBody(t, second, http.StatusConflict, "sanction_not_active")
}

// TestLiftKickConflict proves kicks are never liftable.
func TestLiftKickConflict(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(models.Sanction{
		ID:           sanctionID,
		CommunityID:  communityID,
		TargetUserID: targetID,
		Type:         models.SanctionTypeKick,
		Source:       models.SourceModerator,
		IssuedAt:     nowTime(),
	})

	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/sanctions/"+sanctionID+"/lift", "", moderatorID, nil)
	assertErrorBody(t, recorder, http.StatusConflict, "kick_not_liftable")
}

// TestCreateAppeal covers submission, ownership, appealability, and the open
// appeal guard.
func TestCreateAppeal(t *testing.T) {
	t.Parallel()

	t.Run("member submits", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		f.store.Sanctions.Add(activeBan())

		recorder := f.do(t, http.MethodPost, "/sanctions/"+sanctionID+"/appeals", `{"statement":"please review"}`, targetID, nil)
		require.Equal(t, http.StatusCreated, recorder.Code, "body: %s", recorder.Body.String())

		var appeal models.Appeal
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &appeal))
		assert.Equal(t, models.AppealStatusSubmitted, appeal.Status)
		assert.Equal(t, targetID, appeal.AppellantUserID)
		assert.Len(t, f.store.Outbox.All(), 1)
	})

	t.Run("stranger cannot appeal", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		f.store.Sanctions.Add(activeBan())

		recorder := f.do(t, http.MethodPost, "/sanctions/"+sanctionID+"/appeals", `{"statement":"let me in"}`, moderatorID, nil)
		assertErrorBody(t, recorder, http.StatusForbidden, "not_sanction_target")
	})

	t.Run("kick is not appealable", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		f.store.Sanctions.Add(models.Sanction{
			ID:           sanctionID,
			CommunityID:  communityID,
			TargetUserID: targetID,
			Type:         models.SanctionTypeKick,
			Source:       models.SourceModerator,
			IssuedAt:     nowTime(),
		})

		recorder := f.do(t, http.MethodPost, "/sanctions/"+sanctionID+"/appeals", `{"statement":"why"}`, targetID, nil)
		assertErrorBody(t, recorder, http.StatusUnprocessableEntity, "sanction_not_appealable")
	})

	t.Run("second open appeal conflicts", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		f.store.Sanctions.Add(activeBan())
		f.store.Appeals.Add(models.Appeal{
			ID:              appealID,
			SanctionID:      sanctionID,
			CommunityID:     communityID,
			AppellantUserID: targetID,
			Statement:       "first",
			Status:          models.AppealStatusSubmitted,
			CreatedAt:       nowTime(),
		})

		recorder := f.do(t, http.MethodPost, "/sanctions/"+sanctionID+"/appeals", `{"statement":"second"}`, targetID, nil)
		assertErrorBody(t, recorder, http.StatusConflict, "appeal_already_open")
	})

	t.Run("unknown sanction is 404", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		recorder := f.do(t, http.MethodPost, "/sanctions/"+sanctionID+"/appeals", `{"statement":"why"}`, targetID, nil)
		assertErrorBody(t, recorder, http.StatusNotFound, "sanction_not_found")
	})
}

// TestMemberViewsNeedNoAuthorizationCall proves /me/* never calls API-001.
func TestMemberViewsNeedNoAuthorizationCall(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(activeBan())
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "mine",
		Status:          models.AppealStatusOverturned,
		CreatedAt:       nowTime(),
	})

	sanctions := f.do(t, http.MethodGet, "/me/sanctions", "", targetID, nil)
	require.Equal(t, http.StatusOK, sanctions.Code, "body: %s", sanctions.Body.String())
	var mine models.MySanctionPage
	require.NoError(t, json.Unmarshal(sanctions.Body.Bytes(), &mine))
	require.Len(t, mine.Data, 1)
	assert.Equal(t, models.SanctionTypeBan, mine.Data[0].Type)

	appeals := f.do(t, http.MethodGet, "/me/appeals?limit=10", "", targetID, nil)
	require.Equal(t, http.StatusOK, appeals.Code, "body: %s", appeals.Body.String())
	var appealPage models.AppealPage
	require.NoError(t, json.Unmarshal(appeals.Body.Bytes(), &appealPage))
	require.Len(t, appealPage.Data, 1)

	assert.Zero(t, f.transport.GetCallCountInfo()["POST "+checkEndpoint], "member views must not call API-001")
}

// TestCommunityAppealQueue covers the review_appeals queue and its denial.
func TestCommunityAppealQueue(t *testing.T) {
	t.Parallel()

	t.Run("queue with filter", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		f.store.Appeals.Add(models.Appeal{
			ID:              appealID,
			SanctionID:      sanctionID,
			CommunityID:     communityID,
			AppellantUserID: targetID,
			Statement:       "queue",
			Status:          models.AppealStatusSubmitted,
			CreatedAt:       nowTime(),
		})

		recorder := f.do(t, http.MethodGet, "/communities/"+communityID+"/appeals?status=submitted", "", reviewerID, nil)
		require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

		var page models.AppealPage
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
		require.Len(t, page.Data, 1)
	})

	t.Run("denied reviewer", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, denyResponder("missing_permission"))
		recorder := f.do(t, http.MethodGet, "/communities/"+communityID+"/appeals", "", reviewerID, nil)
		assertErrorBody(t, recorder, http.StatusForbidden, "insufficient_permission")
	})
}

// TestDecideAppealOverturned covers the 200 decision and the overturn cascade.
func TestDecideAppealOverturned(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(activeBan())
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "unfair",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       nowTime(),
	})

	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/appeals/"+appealID+"/decision",
		`{"decision":"overturned","note":"misidentified"}`, reviewerID, nil)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var decided models.Appeal
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &decided))
	assert.Equal(t, models.AppealStatusOverturned, decided.Status)

	stored, ok := f.store.Sanctions.ByID(sanctionID)
	require.True(t, ok)
	require.NotNil(t, stored.LiftSource)
	assert.Equal(t, models.SourceAppeal, *stored.LiftSource)
	assert.Len(t, f.store.Outbox.All(), 2, "an overturn emits EVT-004 and EVT-006")
	assert.Equal(t, 1, f.store.TxCount)
}

// TestDecideAppealConflicts covers the 409 for an already decided appeal.
func TestDecideAppealConflicts(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	f.store.Sanctions.Add(activeBan())
	decidedAt := nowTime().Add(-time.Hour)
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "handled",
		Status:          models.AppealStatusUpheld,
		ReviewerUserID:  ptr(reviewerID),
		DecidedAt:       &decidedAt,
		CreatedAt:       nowTime().Add(-2 * time.Hour),
	})

	recorder := f.do(t, http.MethodPost, "/communities/"+communityID+"/appeals/"+appealID+"/decision",
		`{"decision":"upheld"}`, reviewerID, nil)
	assertErrorBody(t, recorder, http.StatusConflict, "appeal_already_decided")
}

// TestAutomodEndpoints covers 404, 422, and the versioned PUT response.
func TestAutomodEndpoints(t *testing.T) {
	t.Parallel()

	validBody := `{"spam":{"enabled":true,"action":"block","max_messages_per_window":5,"window_seconds":10,` +
		`"max_duplicate_messages":2,"max_mentions_per_message":3,"max_links_per_message":1},` +
		`"banned_words":{"enabled":true,"action":"remove","match_mode":"whole_word","words":["spam"]},` +
		`"exempt_role_ids":[]}`

	t.Run("not configured is 404", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		recorder := f.do(t, http.MethodGet, "/communities/"+communityID+"/automod", "", moderatorID, nil)
		assertErrorBody(t, recorder, http.StatusNotFound, "automod_not_configured")
	})

	t.Run("put stores version 1 then 2", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())

		first := f.do(t, http.MethodPut, "/communities/"+communityID+"/automod", validBody, moderatorID, nil)
		require.Equal(t, http.StatusOK, first.Code, "body: %s", first.Body.String())
		var ruleSet models.AutomodRuleSet
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &ruleSet))
		assert.Equal(t, 1, ruleSet.RuleSetVersion)

		second := f.do(t, http.MethodPut, "/communities/"+communityID+"/automod", validBody, moderatorID, nil)
		require.Equal(t, http.StatusOK, second.Code)
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &ruleSet))
		assert.Equal(t, 2, ruleSet.RuleSetVersion)

		current := f.do(t, http.MethodGet, "/communities/"+communityID+"/automod", "", moderatorID, nil)
		require.Equal(t, http.StatusOK, current.Code, "body: %s", current.Body.String())
		var stored models.AutomodRuleSet
		require.NoError(t, json.Unmarshal(current.Body.Bytes(), &stored))
		assert.Equal(t, 2, stored.RuleSetVersion)
	})

	t.Run("invalid config is 422", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, allowResponder())
		badBody := `{"spam":{"enabled":true,"action":"block","max_messages_per_window":5,"window_seconds":0,` +
			`"max_duplicate_messages":2,"max_mentions_per_message":3,"max_links_per_message":1},` +
			`"banned_words":{"enabled":true,"action":"remove","match_mode":"whole_word","words":[]},` +
			`"exempt_role_ids":[]}`

		recorder := f.do(t, http.MethodPut, "/communities/"+communityID+"/automod", badBody, moderatorID, nil)
		assertErrorBody(t, recorder, http.StatusUnprocessableEntity, "invalid_automod_config")
		assert.Empty(t, f.store.Outbox.All())
	})
}

// TestAuditLogRead covers the 365-day filter, the action filter, and the actor
// filter.
func TestAuditLogRead(t *testing.T) {
	t.Parallel()

	f := newRouterFixture(t, allowResponder())
	recent := nowTime().Add(-24 * time.Hour)
	outOfWindow := nowTime().Add(-models.AuditRetention - 24*time.Hour)
	f.store.Audit.Add(models.AuditLogEntry{
		ID:          "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		CommunityID: communityID,
		Action:      models.AuditActionSanctionIssued,
		CreatedAt:   outOfWindow,
	})
	f.store.Audit.Add(models.AuditLogEntry{
		ID:          "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		CommunityID: communityID,
		ActorUserID: ptr(moderatorID),
		Action:      models.AuditActionSanctionLifted,
		CreatedAt:   recent,
	})

	recorder := f.do(t, http.MethodGet, "/communities/"+communityID+"/audit-log", "", moderatorID, nil)
	require.Equal(t, http.StatusOK, recorder.Code, "body: %s", recorder.Body.String())

	var page models.AuditLogPage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
	require.Len(t, page.Data, 1)
	assert.Equal(t, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", page.Data[0].ID)

	filtered := f.do(t, http.MethodGet, "/communities/"+communityID+"/audit-log?action_type=sanction_issued", "", moderatorID, nil)
	require.Equal(t, http.StatusOK, filtered.Code)
	var empty models.AuditLogPage
	require.NoError(t, json.Unmarshal(filtered.Body.Bytes(), &empty))
	assert.Empty(t, empty.Data)

	byActor := f.do(t, http.MethodGet, "/communities/"+communityID+"/audit-log?actor="+moderatorID, "", moderatorID, nil)
	require.Equal(t, http.StatusOK, byActor.Code)
	var actorPage models.AuditLogPage
	require.NoError(t, json.Unmarshal(byActor.Body.Bytes(), &actorPage))
	require.Len(t, actorPage.Data, 1)
}

// TestProbesAndInfrastructureRoutes covers /healthz, /readyz, /metrics, and the
// error shapes of unknown routes and methods.
func TestProbesAndInfrastructureRoutes(t *testing.T) {
	t.Parallel()

	t.Run("healthz is liveness only", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		recorder := f.do(t, http.MethodGet, "/healthz", "", "", nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		var status models.HealthStatus
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &status))
		assert.Equal(t, "ok", status.Status)
	})

	t.Run("healthz ignores dependency failures", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		f.readyChecker.err = &models.Error{Status: http.StatusServiceUnavailable, Code: "not_ready", Detail: "db down"}
		require.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/healthz", "", "", nil).Code)
	})

	t.Run("readyz reports readiness", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		require.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/readyz", "", "", nil).Code)
	})

	t.Run("readyz fails when a dependency is down", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		f.readyChecker.err = &models.Error{
			Status: http.StatusServiceUnavailable,
			Code:   "not_ready",
			Detail: "The database is unavailable.",
		}
		assertErrorBody(t, f.do(t, http.MethodGet, "/readyz", "", "", nil), http.StatusServiceUnavailable, "not_ready")
	})

	t.Run("metrics is exposed with RED series", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		f.do(t, http.MethodGet, "/healthz", "", "", nil)
		recorder := f.do(t, http.MethodGet, "/metrics", "", "", nil)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "http_requests_total")
		assert.Contains(t, recorder.Body.String(), `route="/healthz"`)
	})

	t.Run("unknown route", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		assertErrorBody(t, f.do(t, http.MethodGet, "/v1/nope", "", moderatorID, nil), http.StatusNotFound, "not_found")
	})

	t.Run("method not allowed", func(t *testing.T) {
		t.Parallel()
		f := newRouterFixture(t, nil)
		assertErrorBody(t, f.do(t, http.MethodDelete, "/me/sanctions", "", moderatorID, nil), http.StatusMethodNotAllowed, "method_not_allowed")
	})
}

// TestReadyModel covers the readiness model: database, producer, and shutdown.
func TestReadyModel(t *testing.T) {
	t.Parallel()

	t.Run("shutdown flips readiness", func(t *testing.T) {
		t.Parallel()
		shutdown := handler.NewShutdownState()
		ready := &handler.Ready{Shutdown: shutdown}
		require.NoError(t, ready.Ready(context.Background()))

		shutdown.Begin()
		require.Error(t, ready.Ready(context.Background()))
		assert.True(t, shutdown.ShuttingDown())
	})

	t.Run("database failure", func(t *testing.T) {
		t.Parallel()
		ready := &handler.Ready{Database: &stubDatabase{err: models.ErrInternal}}
		assert.Error(t, ready.Ready(context.Background()))
	})

	t.Run("producer failure", func(t *testing.T) {
		t.Parallel()
		ready := &handler.Ready{Producer: &mocks.Producer{HealthyResult: false}}
		assert.Error(t, ready.Ready(context.Background()))
	})

	t.Run("all healthy", func(t *testing.T) {
		t.Parallel()
		ready := &handler.Ready{
			Database: &stubDatabase{},
			Producer: &mocks.Producer{HealthyResult: true},
			Shutdown: handler.NewShutdownState(),
		}
		assert.NoError(t, ready.Ready(context.Background()))
	})
}
