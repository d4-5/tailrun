package handler

import (
	"io/fs"
	"net/http"
)

type StaticHandler struct {
	fs fs.FS
}

func NewStaticHandler(fs fs.FS) *StaticHandler {
	return &StaticHandler{fs: fs}
}

func (h *StaticHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/", http.FileServer(http.FS(h.fs)))
}
