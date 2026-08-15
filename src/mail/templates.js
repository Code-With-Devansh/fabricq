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
