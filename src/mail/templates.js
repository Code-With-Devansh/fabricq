function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

export function teamInvitationTemplate({
  teamName,
  inviterEmail,
  roleName,
  acceptUrl,
  expiresAt,
}) {
  const safeTeam = escapeHtml(teamName);
  const safeInviter = escapeHtml(inviterEmail);
  const safeRole = escapeHtml(roleName);
  const expiresLabel = new Date(expiresAt).toDateString();

  const subject = `${safeInviter} invited you to join ${safeTeam} on FabricQ`;

  const html = `
    <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
      <h2>You've been invited to ${safeTeam}</h2>
      <p>${safeInviter} invited you to join <strong>${safeTeam}</strong> on FabricQ as <strong>${safeRole}</strong>.</p>
      <p>
        <a href="${acceptUrl}" style="display:inline-block;padding:10px 20px;background:#111;color:#fff;text-decoration:none;border-radius:6px;">
          Accept invitation
        </a>
      </p>
      <p style="color:#666;font-size:13px;">This invitation expires on ${expiresLabel}. If you weren't expecting this, you can ignore this email.</p>
    </div>
  `;

  const text = `${inviterEmail} invited you to join ${teamName} on FabricQ as ${roleName}.\n\nAccept: ${acceptUrl}\n\nThis invitation expires on ${expiresLabel}.`;

  return { subject, html, text };
}

export function emailVerificationTemplate({ verifyUrl, expiresAt }) {
  const expiresLabel = new Date(expiresAt).toDateString();
  const subject = "Verify your email for FabricQ";

  const html = `
    <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
      <h2>Verify your email</h2>
      <p>Confirm this is your email address to finish setting up your FabricQ account.</p>
      <p>
        <a href="${verifyUrl}" style="display:inline-block;padding:10px 20px;background:#111;color:#fff;text-decoration:none;border-radius:6px;">
          Verify email
        </a>
      </p>
      <p style="color:#666;font-size:13px;">This link expires on ${expiresLabel}. If you didn't create a FabricQ account, you can ignore this email.</p>
    </div>
  `;

  const text = `Verify your email for FabricQ: ${verifyUrl}\n\nThis link expires on ${expiresLabel}.`;

  return { subject, html, text };
}

export function passwordResetTemplate({ resetUrl, expiresAt }) {
  const expiresLabel = new Date(expiresAt).toDateString();
  const subject = "Reset your FabricQ password";

  const html = `
    <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
      <h2>Reset your password</h2>
      <p>We got a request to reset the password on your FabricQ account.</p>
      <p>
        <a href="${resetUrl}" style="display:inline-block;padding:10px 20px;background:#111;color:#fff;text-decoration:none;border-radius:6px;">
          Reset password
        </a>
      </p>
      <p style="color:#666;font-size:13px;">This link expires on ${expiresLabel}. If you didn't request this, you can ignore this email - your password won't change.</p>
    </div>
  `;

  const text = `Reset your FabricQ password: ${resetUrl}\n\nThis link expires on ${expiresLabel}. If you didn't request this, ignore this email.`;

  return { subject, html, text };
}

export function twoFactorCodeTemplate({ code, expiresAt }) {
  const expiresMinutes = Math.round((new Date(expiresAt) - Date.now()) / 60000);
  const subject = `${code} is your FabricQ verification code`;

  const html = `
    <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
      <h2>Your verification code</h2>
      <p style="font-size:32px;font-weight:bold;letter-spacing:4px;">${code}</p>
      <p style="color:#666;font-size:13px;">This code expires in ${expiresMinutes} minutes. If you didn't try to sign in, you can ignore this email.</p>
    </div>
  `;

  const text = `Your FabricQ verification code is ${code}. It expires in ${expiresMinutes} minutes.`;

  return { subject, html, text };
}
