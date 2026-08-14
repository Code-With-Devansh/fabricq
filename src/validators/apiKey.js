import { z } from "zod";
import { API_KEY_SCOPES } from "../services/apiKey.service.js";

export const createApiKeySchema = z.object({
  name: z.string().trim().min(1).max(200),
  scopes: z.array(z.enum(API_KEY_SCOPES)).min(1),
  expires_at: z.coerce.date().optional(),
});
