import config from "../config/index.js";

const { name, path, sameSite, secure } = config.auth.refreshCookie;

export function setRefreshCookie(res, refreshToken) {
  res.cookie(name, refreshToken, {
    httpOnly: true,
    secure,
    sameSite,
    path,
    maxAge: config.auth.refreshTokenTtlSeconds * 1000,
  });
}

export function clearRefreshCookie(res) {
  res.clearCookie(name, { httpOnly: true, secure, sameSite, path });
}

export function getRefreshCookie(req) {
  return req.cookies?.[name] ?? null;
}
