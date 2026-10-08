package handler

import (
	"io/fs"
	"net/http"

	"github.com/d4-5/tailrun/internal/web"
)

type StaticHandler struct {
	fs fs.FS
}

func NewStaticHandler() *StaticHandler {
	return &StaticHandler{fs: web.FS}
}

func (h *StaticHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/", http.FileServer(http.FS(h.fs)))
}
