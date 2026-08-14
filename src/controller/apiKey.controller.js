import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import { createApiKeySchema } from "../validators/apiKey.js";
import {
  listApiKeysService,
  createApiKeyService,
  revokeApiKeyService,
} from "../services/apiKey.service.js";

export const listApiKeys = asyncHandler(async (req, res) => {
  const keys = await listApiKeysService(req.team.teamId);
  return res.status(200).json({ success: true, data: keys });
});

export const createApiKey = asyncHandler(async (req, res) => {
  const validated = createApiKeySchema.safeParse(req.body);
  console.log("$$$$$$$$$$$")
  console.log(validated)
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const key = await createApiKeyService({
    teamId: req.team.teamId,
    name: validated.data.name,
    requestedScopes: validated.data.scopes,
    expiresAt: validated.data.expires_at ?? null,
    createdBy: req.auth.userId,
    creatorPermissions: req.team.permissions,
  });

  // key.api_key is the raw secret - only ever returned here, once.
  return res.status(201).json({ success: true, data: key });
});

export const revokeApiKey = asyncHandler(async (req, res) => {
  await revokeApiKeyService({
    teamId: req.team.teamId,
    keyId: req.params.keyId,
  });
  return res.status(204).send();
});
