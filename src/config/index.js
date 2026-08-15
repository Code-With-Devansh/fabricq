export default {
  app: {
    env: process.env.NODE_ENV,
    port: Number(process.env.PORT),
  },
  redis: {
    url: process.env.REDIS_URL,
  },
  logging: {
    level: process.env.LOG_LEVEL,
  },
  postgres: {
    url: process.env.DATABASE_URL,
    max: Number(process.env.DATABASE_POOL_MAX) || 10,
    idleTimeoutMillis: Number(process.env.DATABASE_IDLE_TIMEOUT_MS) || 30_000,
    connectionTimeoutMillis:
      Number(process.env.DATABASE_CONNECT_TIMEOUT_MS) || 10_000,
  },
  auth: {
    jwtSecret: process.env.JWT_ACCESS_SECRET,
    accessTokenTtlSeconds: Number(process.env.JWT_ACCESS_TTL_SECONDS) || 300, // 5 min
    refreshTokenTtlSeconds:
      Number(process.env.REFRESH_TOKEN_TTL_SECONDS) || 60 * 60 * 24 * 7, // 7 days
    inviteTokenTtlSeconds:
      Number(process.env.INVITE_TOKEN_TTL_SECONDS) || 60 * 60 * 24 * 7, // 7 days
    issuer: process.env.JWT_ISSUER || "fabricq",
    audience: process.env.JWT_AUDIENCE || "fabricq-dashboard",
    argon2: {
      // argon2id, OWASP-recommended baseline params
      memoryCost: Number(process.env.ARGON2_MEMORY_COST) || 19_456, // ~19 MiB
      timeCost: Number(process.env.ARGON2_TIME_COST) || 2,
      parallelism: Number(process.env.ARGON2_PARALLELISM) || 1,
    },
    refreshCookie: {
      name: process.env.REFRESH_COOKIE_NAME || "fq_refresh",
      // Scoped to /auth so the cookie isn't attached to every request
      // (e.g. /jobs, /health) - only refresh/logout need to see it.
      path: "/auth",
      // "none" is required for cross-origin dashboard <-> API deployments
      // (mirrors the Snip dual-token pattern); requires secure: true.
      sameSite: process.env.REFRESH_COOKIE_SAMESITE || "lax",
      secure: process.env.NODE_ENV === "production",
    },
  },
  // Dashboard origin allowed to send credentialed (cookie-bearing)
  // requests. Wildcard CORS is incompatible with cookies, so this must
  // be an explicit origin in production.
  dashboardOrigin: process.env.DASHBOARD_ORIGIN || "http://localhost:5173",
};
