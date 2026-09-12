package auth

import (
	"errors"
	"github.com/golang-jwt/jwt/v5"
	"notifier/internal/config"
	"strings"
)

type Claims struct {
	ClientID string `json:"client_id"`
	jwt.RegisteredClaims
}

func Authenticate(header string, s *config.Snapshot) (string, error) {
	if len(header) > 16384 || !strings.HasPrefix(header, "Bearer ") || s == nil {
		return "", errors.New("invalid bearer token")
	}
	raw := strings.TrimPrefix(header, "Bearer ")
	untrusted := new(Claims)
	if _, _, err := jwt.NewParser().ParseUnverified(raw, untrusted); err != nil {
		return "", errors.New("invalid JWT")
	}
	issuer, ok := s.Issuers[untrusted.Issuer]
	if !ok {
		return "", errors.New("unknown issuer")
	}
	claims := new(Claims)
	token, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		key, ok := s.Keys[untrusted.Issuer][kid]
		if !ok {
			return nil, errors.New("unknown signing key")
		}
		return key, nil
	}, jwt.WithValidMethods([]string{issuer.Algorithm}), jwt.WithIssuer(untrusted.Issuer), jwt.WithAudience(issuer.Audience), jwt.WithExpirationRequired())
	if err != nil || !token.Valid || claims.Subject == "" {
		return "", errors.New("JWT validation failed")
	}
	c, ok := s.Clients[claims.ClientID]
	if !ok || !c.Enabled {
		return "", errors.New("client disabled")
	}
	return claims.ClientID, nil
}
