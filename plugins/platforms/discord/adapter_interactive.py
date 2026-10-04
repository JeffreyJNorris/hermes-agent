"""Discord complete-set controls, sharing existing prompt lifecycle handling."""

from gateway.interactive import (
    MAX_MULTI_SELECT_OPTIONS,
    ResolveResult,
    ResolveStatus,
    expire_prompt,
    make_multi_select_component_id,
    parse_multi_select_component_id,
    remaining_prompt_seconds,
    resolve_multi_select_prompt,
)


def make_multi_select_control(discord, options, custom_id, minimum, maximum):
    from .adapter import _DISCORD_SELECT_FIELD_LIMIT, _truncate_discord_component_text

    if (not 2 <= len(options) <= MAX_MULTI_SELECT_OPTIONS
            or type(minimum) is not int or type(maximum) is not int
            or not 1 <= minimum <= maximum <= len(options)):
        raise ValueError("invalid multi-select bounds")
    return discord.ui.Select(
        custom_id=custom_id,
        min_values=minimum,
        max_values=maximum,
        placeholder=f"Choose {minimum}–{maximum} options",
        options=[
            discord.SelectOption(
                label=_truncate_discord_component_text(option.label, _DISCORD_SELECT_FIELD_LIMIT),
                value=str(slot),
            )
            for slot, option in enumerate(options, 1)
        ],
    )


def define_multi_select_view(discord, single_view):
    """Lazy Discord dependency; inherit auth, acknowledgement, edits and expiry."""

    class InteractiveMultiSelectView(single_view):
        def __init__(self, prompt, allowed_user_ids, allowed_role_ids=None):
            remaining = remaining_prompt_seconds(prompt.prompt_id)
            timeout = prompt.timeout_seconds if remaining is None else remaining
            discord.ui.View.__init__(self, timeout=max(0.001, float(timeout)))
            self.prompt_id = prompt.prompt_id
            self.allowed_user_ids = allowed_user_ids
            self.allowed_role_ids = allowed_role_ids or set()
            self._message = None
            control = make_multi_select_control(
                discord, prompt.options, make_multi_select_component_id(prompt.prompt_id),
                prompt.min_selections, prompt.max_selections,
            )

            async def callback(interaction):
                await self._resolve(interaction, "")

            control.callback = callback
            self.add_item(control)

        def _resolve_selection(self, interaction, slot, **metadata):
            data = interaction.data if isinstance(getattr(interaction, "data", None), dict) else {}
            values = data.get("values")
            if (parse_multi_select_component_id(data.get("custom_id")) != self.prompt_id
                    or not isinstance(values, list) or not all(isinstance(value, str) for value in values)):
                return ResolveResult(ResolveStatus.INVALID_OPTION)
            return resolve_multi_select_prompt(
                prompt_id=self.prompt_id, option_tokens=tuple(values), **metadata,
            )

        def _expire_prompt(self):
            return expire_prompt(self.prompt_id, only_if_due=True)

    return InteractiveMultiSelectView
