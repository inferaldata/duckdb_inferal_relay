# Agent Instructions

## Clear commit messages (required)

Keep the repository's commit format, including Problem/Solution where required.
Use plain language inspired by ASD-STE100: common words, short active sentences,
and one idea per sentence. Explain what goes wrong and what changes. Use the
same word for the same thing. Explain necessary technical terms; avoid jargon,
stacked nouns, and vague abstractions. Prefer clarity to a rigid subject length.
This is guidance inspired by STE, not a claim of full compliance with its dictionary.

Before each commit, ask a separate subagent to review the exact proposed message.
Start that reviewer without inherited task or conversation context. Give it only
the message and general project context, such as the project's purpose and stack.
It may read project context, but must not inspect the diff, changed files, task
conversation, or an explanation of this change. Ask it to restate the problem and
solution and flag unclear words. If it cannot explain both, rewrite the message
and repeat the review before committing. The author must check accuracy against
the diff separately. If isolated subagent review is unavailable, report that and
do not claim that the review passed.
