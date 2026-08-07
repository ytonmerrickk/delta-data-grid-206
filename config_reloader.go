package main

import (
	"context"
	"log/slog"
	"os"
	"time"
)

const configReloadInterval = 60 * time.Second

type configReloader struct {
	tokenPath   string
	channelPath string
	auth        *keyAuth
	channel     *channelStateStore
	logger      *slog.Logger
	tokenMod    time.Time
	channelMod  time.Time
}

func newConfigReloader(tokenPath, channelPath string, auth *keyAuth, channel *channelStateStore, logger *slog.Logger) *configReloader {
	reloader := &configReloader{
		tokenPath:   tokenPath,
		channelPath: channelPath,
		auth:        auth,
		channel:     channel,
		logger:      logger,
	}
	if info, err := os.Stat(tokenPath); err == nil {
		reloader.tokenMod = info.ModTime()
	}
	if info, err := os.Stat(channelPath); err == nil {
		reloader.channelMod = info.ModTime()
	}
	return reloader
}

func (r *configReloader) Start(ctx context.Context) {
	if r == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(configReloadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.reloadChanged()
			}
		}
	}()
}

func (r *configReloader) reloadChanged() {
	r.reloadTokens()
	r.reloadChannels()
}

func (r *configReloader) reloadTokens() {
	if r.auth == nil {
		return
	}
	info, err := os.Stat(r.tokenPath)
	if err != nil {
		r.logger.Warn("检查 token 配置失败", "path", r.tokenPath, "error", err)
		return
	}
	if !info.ModTime().After(r.tokenMod) {
		return
	}
	cfg, err := loadTokenConfig(r.tokenPath)
	if err != nil {
		r.logger.Warn("重新读取 token 配置失败", "path", r.tokenPath, "error", err)
		return
	}
	r.auth.Replace(cfg.Tokens)
	r.tokenMod = info.ModTime()
	r.logger.Info("token 配置已热更新", "path", r.tokenPath, "tokens", len(cfg.Tokens))
}

func (r *configReloader) reloadChannels() {
	if r.channel == nil {
		return
	}
	info, err := os.Stat(r.channelPath)
	if err != nil {
		r.logger.Warn("检查 channel 配置失败", "path", r.channelPath, "error", err)
		return
	}
	if !info.ModTime().After(r.channelMod) {
		return
	}
	cfg, err := loadChannelConfig(r.channelPath)
	if err != nil {
		r.logger.Warn("重新读取 channel 配置失败", "path", r.channelPath, "error", err)
		return
	}
	r.channel.ReplaceConfig(cfg)
	r.channelMod = info.ModTime()
	r.logger.Info("channel 配置已热更新", "path", r.channelPath, "channels", len(cfg.Channels))
}
