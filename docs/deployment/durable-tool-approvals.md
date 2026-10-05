# Durable tool approvals

Discord terminal and Python execution approvals can use the existing durable request and trusted-action system. The review appears in the originating conversation or thread. These requests never fall back to the configured approvals inbox. Only the user who initiated the operation can approve it.

When a tool needs approval, it saves the exact operation privately and returns a pending receipt without running it. The agent can finish its turn while waiting. Approve runs that operation once, and its bounded output resumes the original conversation. Reject leaves it unexecuted. Buttons survive gateway restarts. An ambiguous interrupted execution is recorded as unknown rather than retried automatically.

The compact terminal/code card preserves the original controls: **Allow Once**, **Allow Session**, **Always Allow**, and **Deny**. Session and permanent choices use the original guard policy and pattern scope; smart-denial overrides and findings that cannot be permanently allowed retain those restrictions. A durable session grant belongs to the original physical conversation and actor, survives a gateway restart, and ends on session reset. Always Allow uses the existing profile configuration allowlist. The review card updates with the execution outcome while Hermes receives the bounded tool result in the same conversation.

Enable the desired actions in `config.yaml`:

```yaml
durable_requests:
  enabled: true
  max_ttl_seconds: 604800
durable_approvals:
  enabled: true
  allowed_actions:
    - cron.resume
    - terminal.execute
    - execute_code.execute
    - host.sudo
  execution_ttl_seconds: 604800
  host_sudo_allowed_users: ["YOUR_DISCORD_USER_ID"]
```

The execution review remains answerable for seven days with these settings. This deadline is separate from `approvals.timeout`, which still governs synchronous approval surfaces. Existing hard blocks remain in force. A changed execution environment invalidates a pending operation; Python code also requires its original kernel state to remain available. A closed conversation cannot be used to execute an old approval.

## Host sudo

The dedicated SSH operator retains its existing limited sudoers permissions. Its exact permitted service commands can run directly. Extra flags may fall outside that allowlist: use the documented command and filter its output afterward.

For a different privileged host command, have the agent submit this through its terminal tool:

```bash
hermes host sudo -- /usr/bin/systemctl --user --machine=jeff@.host show hermes-google-tasks.timer -p ActiveState
```

This is an explicit request marker intercepted by Hermes. The reviewed command executes on the gateway host through the gateway owner's existing sudo privileges after approval. It does not execute inside Docker, transmit a sudo password, or grant the SSH operator general root access. Arguments are passed directly without shell expansion. Shell operations require an explicit shell executable and appear in the review.

The standalone CLI prints guidance and refuses execution: it cannot bypass Discord approval. The approval authorizes the reviewed command once, rather than future commands or a permanent privilege grant.

Host sudo uses a distinct red **HOST SUDO - elevated privileges** card with **Approve Once** and **Deny** only. Every host sudo approval requires its operator's explicit decision. Session grants, permanent allowances, yolo, disabled ordinary approvals, and unattended auto-approval never authorize this action. Both approval variants use text labels without emojis.

Privileged host requests require the initiating user's ID in `host_sudo_allowed_users`. This is checked before requesting approval and again before executing it. Allowing someone to chat with the gateway does not give them this permission.
