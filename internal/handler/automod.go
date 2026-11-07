package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"moderation_service/internal/models"
)

// getAutomod handles GET /communities/{community_id}/automod.
func (h *Handlers) getAutomod(c *gin.Context) {
	ruleSet, err := h.Automod.Get(c.Request.Context(), actorID(c), c.Param("community_id"))
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, ruleSet)
}

// putAutomod handles PUT /communities/{community_id}/automod.
func (h *Handlers) putAutomod(c *gin.Context) {
	var request models.AutomodRuleSetInput
	if err := bindJSON(c, &request, false); err != nil {
		respondError(c, err)
		return
	}
	key, err := idempotencyKey(c)
	if err != nil {
		respondError(c, err)
		return
	}
	ruleSet, err := h.Automod.Put(c.Request.Context(), actorID(c), c.Param("community_id"), request, key)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, ruleSet)
}

// listAuditLog handles GET /communities/{community_id}/audit-log.
func (h *Handlers) listAuditLog(c *gin.Context) {
	page, err := pageParam(c)
	if err != nil {
		respondError(c, err)
		return
	}
	var action *models.AuditAction
	if raw := optionalQuery(c, "action_type"); raw != nil {
		value := models.AuditAction(*raw)
		action = &value
	}
	actor := optionalQuery(c, "actor")
	result, err := h.Audit.List(c.Request.Context(), actorID(c), c.Param("community_id"), action, actor, page)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// liveness handles GET /healthz: process responsiveness only, never a
// dependency check, so a transient outage cannot restart the container.
func (h *Handlers) liveness(c *gin.Context) {
	c.JSON(http.StatusOK, models.NewHealthStatus())
}

// readiness handles GET /readyz: database ping, Kafka producer availability,
// and the shutdown flag.
func (h *Handlers) readiness(c *gin.Context) {
	if h.Readiness != nil {
		if err := h.Readiness.Ready(c.Request.Context()); err != nil {
			respondError(c, err)
			return
		}
	}
	c.JSON(http.StatusOK, models.NewHealthStatus())
}
