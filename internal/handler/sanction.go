package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"moderation_service/internal/models"
)

// createSanction handles POST /communities/{community_id}/sanctions.
func (h *Handlers) createSanction(c *gin.Context) {
	var request models.CreateSanctionRequest
	if err := bindJSON(c, &request, false); err != nil {
		respondError(c, err)
		return
	}
	key, err := idempotencyKey(c)
	if err != nil {
		respondError(c, err)
		return
	}
	sanction, err := h.Sanctions.Issue(c.Request.Context(), actorID(c), c.Param("community_id"), request, key)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusCreated, sanction)
}

// listSanctions handles GET /communities/{community_id}/sanctions.
func (h *Handlers) listSanctions(c *gin.Context) {
	page, err := pageParam(c)
	if err != nil {
		respondError(c, err)
		return
	}
	var status *models.SanctionStatus
	if raw := optionalQuery(c, "status"); raw != nil {
		value := models.SanctionStatus(*raw)
		status = &value
	}
	result, err := h.Sanctions.List(c.Request.Context(), actorID(c), c.Param("community_id"), status, page)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// liftSanction handles POST /communities/{community_id}/sanctions/{sanction_id}/lift.
func (h *Handlers) liftSanction(c *gin.Context) {
	var request models.LiftSanctionRequest
	if err := bindJSON(c, &request, true); err != nil {
		respondError(c, err)
		return
	}
	sanction, err := h.Sanctions.Lift(c.Request.Context(), actorID(c),
		c.Param("community_id"), c.Param("sanction_id"), request)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, sanction)
}

// listMySanctions handles GET /me/sanctions.
func (h *Handlers) listMySanctions(c *gin.Context) {
	result, err := h.Sanctions.Mine(c.Request.Context(), actorID(c))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}
