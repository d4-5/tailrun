package web

import "embed"

//go:embed index.html index.js style.css
var FS embed.FS
