package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"moderation_service/internal/models"
)

// createAppeal handles POST /sanctions/{sanction_id}/appeals.
func (h *Handlers) createAppeal(c *gin.Context) {
	var request models.CreateAppealRequest
	if err := bindJSON(c, &request, false); err != nil {
		respondError(c, err)
		return
	}
	key, err := idempotencyKey(c)
	if err != nil {
		respondError(c, err)
		return
	}
	appeal, err := h.Appeals.Submit(c.Request.Context(), actorID(c), c.Param("sanction_id"), request, key)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusCreated, appeal)
}

// listMyAppeals handles GET /me/appeals.
func (h *Handlers) listMyAppeals(c *gin.Context) {
	page, err := pageParam(c)
	if err != nil {
		respondError(c, err)
		return
	}
	result, err := h.Appeals.ListMine(c.Request.Context(), actorID(c), page)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// listCommunityAppeals handles GET /communities/{community_id}/appeals.
func (h *Handlers) listCommunityAppeals(c *gin.Context) {
	page, err := pageParam(c)
	if err != nil {
		respondError(c, err)
		return
	}
	var status *models.AppealStatus
	if raw := optionalQuery(c, "status"); raw != nil {
		value := models.AppealStatus(*raw)
		status = &value
	}
	result, err := h.Appeals.ListCommunity(c.Request.Context(), actorID(c), c.Param("community_id"), status, page)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// decideAppeal handles POST /communities/{community_id}/appeals/{appeal_id}/decision.
func (h *Handlers) decideAppeal(c *gin.Context) {
	var request models.AppealDecisionRequest
	if err := bindJSON(c, &request, false); err != nil {
		respondError(c, err)
		return
	}
	key, err := idempotencyKey(c)
	if err != nil {
		respondError(c, err)
		return
	}
	appeal, err := h.Appeals.Decide(c.Request.Context(), actorID(c),
		c.Param("community_id"), c.Param("appeal_id"), request, key)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, appeal)
}
