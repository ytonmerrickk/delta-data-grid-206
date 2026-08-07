package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type channelHealthProbe struct {
	state  *channelStateStore
	logger *slog.Logger
	client *http.Client
	config ProbeConfig
}

func newChannelHealthProbe(state *channelStateStore, logger *slog.Logger, config ProbeConfig) *channelHealthProbe {
	return &channelHealthProbe{
		state:  state,
		logger: logger,
		client: &http.Client{Transport: proxyTransport(), Timeout: config.timeout()},
		config: config,
	}
}

func (p *channelHealthProbe) Start(ctx context.Context) {
	if p == nil || p.state == nil {
		return
	}
	go func() {
		p.runOnce(ctx)
		ticker := time.NewTicker(p.config.interval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				p.logger.Info("探测停止")
				return
			case <-ticker.C:
				p.runOnce(ctx)
			}
		}
	}()
}

func (p *channelHealthProbe) runOnce(ctx context.Context) {
	channels := p.state.ProbeCandidates()
	if len(channels) == 0 {
		return
	}
	p.logger.Info("渠道探测", "channels", len(channels))
	for _, ch := range channels {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ok, status, err := p.probeChannel(ctx, ch)
		if ok {
			if persistErr := p.state.RecordResult(ch.Name, http.StatusOK, nil); persistErr != nil {
				p.logger.Warn("保存探测结果失败",
					"channel", ch.Name,
					"error", persistErr,
				)
			}
			p.logger.Info("探测成功", "channel", ch.Name)
			continue
		}
		if persistErr := p.state.RecordResult(ch.Name, status, err); persistErr != nil {
			p.logger.Warn("保存探测结果失败",
				"channel", ch.Name,
				"status", status,
				"probe_error", err,
				"error", persistErr,
			)
		}
		p.logger.Warn("探测失败",
			"channel", ch.Name,
			"status", status,
			"error", err,
		)
	}
}

func (p *channelHealthProbe) probeChannel(ctx context.Context, ch Channel) (bool, int, error) {
	incoming := &url.URL{Path: "/v1/responses"}
	target, err := buildTargetURL(ch.BaseURL, incoming)
	if err != nil {
		return false, http.StatusBadGateway, err
	}
	payload, err := json.Marshal(map[string]any{
		"model":  p.config.Model,
		"input":  p.config.Prompt,
		"stream": true,
	})
	if err != nil {
		return false, http.StatusBadGateway, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(payload))
	if err != nil {
		return false, http.StatusBadGateway, err
	}
	req.Header.Set("Authorization", "Bearer "+ch.APIKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("User-Agent", p.config.UserAgent)
	req.Header.Set("OpenAI-Beta", p.config.OpenAIBeta)
	req.Header.Set("Originator", p.config.Originator)
	req.Header.Set("Version", p.config.Version)

	resp, err := p.client.Do(req)
	if err != nil {
		return false, http.StatusBadGateway, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, resp.StatusCode, fmt.Errorf("probe upstream status %d", resp.StatusCode)
	}
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		_, _ = io.Copy(io.Discard, resp.Body)
		if p.config.RequireStreaming {
			return false, resp.StatusCode, fmt.Errorf("probe response is not text/event-stream")
		}
		return true, resp.StatusCode, nil
	}
	if err := waitForProbeCompletion(resp.Body); err != nil {
		return false, resp.StatusCode, err
	}
	return true, resp.StatusCode, nil
}

func waitForProbeCompletion(body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 16*1024), 2*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "response.completed") || strings.Contains(line, "response.done") {
			return nil
		}
		if strings.Contains(line, "response.failed") || strings.HasPrefix(line, "event: error") {
			return fmt.Errorf("probe stream failed: %s", line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("probe stream ended before response.completed")
}
