package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/nfnt/resize"
)

const ImageType = "image"

// ImagePayload is the JSON payload for image processing jobs.
type ImagePayload struct {
	SourceURL string `json:"source_url"` // URL to fetch image
	SourcePath string `json:"source_path"` // or local path (for dev)
	Width     uint   `json:"width"`
	Height    uint   `json:"height"`
	OutPath   string `json:"out_path"` // optional; if empty, only resize in memory and discard (for testing)
}

// Image processes images (resize/thumbnail).
type Image struct {
	OutputDir string // optional base dir for OutPath
}

func (i *Image) Type() string { return ImageType }

func (i *Image) Handle(ctx context.Context, payload []byte) error {
	var p ImagePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("image payload: %w", err)
	}
	if p.Width == 0 && p.Height == 0 {
		p.Width = 200
		p.Height = 200
	}
	var img image.Image
	var err error
	if p.SourceURL != "" {
		img, err = fetchImage(ctx, p.SourceURL)
	} else if p.SourcePath != "" {
		img, err = loadImage(p.SourcePath)
	} else {
		return fmt.Errorf("image: need source_url or source_path")
	}
	if err != nil {
		return err
	}
	resized := resize.Thumbnail(p.Width, p.Height, img, resize.Lanczos3)
	if p.OutPath == "" {
		slog.Info("image resized (no out_path)", "width", p.Width, "height", p.Height)
		return nil
	}
	outPath := p.OutPath
	if i.OutputDir != "" && !filepath.IsAbs(outPath) {
		outPath = filepath.Join(i.OutputDir, outPath)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()
	// Re-encode as PNG for simplicity (resize returns image.Image)
	return encodePNG(f, resized)
}

func fetchImage(ctx context.Context, url string) (image.Image, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}
	img, _, err := image.Decode(resp.Body)
	return img, err
}

func loadImage(path string) (image.Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	return img, err
}

func encodePNG(w io.Writer, img image.Image) error {
	return png.Encode(w, img)
}