package api

import (
	"errors"
	"net/http"

	"github.com/chunkgate/chunkgate/internal/s3auth"
)

func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (s3auth.Identity, bool) {
	if s.auth == nil || !s.auth.Enabled() {
		return s3auth.Identity{Tenant: "default"}, true
	}
	identity, err := s.auth.Verify(r)
	if err == nil {
		return identity, true
	}
	var authErr *s3auth.Error
	if errors.As(err, &authErr) {
		writeError(w, authErr.Status, authErr.Code, authErr.Message)
		return s3auth.Identity{}, false
	}
	writeError(w, http.StatusForbidden, "AccessDenied", "access denied")
	return s3auth.Identity{}, false
}
