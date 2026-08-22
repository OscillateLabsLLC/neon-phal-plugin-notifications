# NEON AI (TM) SOFTWARE, Software Development Kit & Application Development System
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2026 Neongecko.com Inc.
# BSD-3
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS;  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE,  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
Requester identity derived from `Message.context`.

The bus carries no authentication, so these are claims, not proof.
Precedence for a consumer identity: hub-stamped `node.node_id`, then
`client_id`, then a non-local `session.session_id` (for Nodes,
`session_id == node_id` today).
"""

from typing import Optional

LOCAL_SESSION_ID = "default"
LOCAL_USERNAME = "local"
FORWARDED_MARKER = "notification_manager_forwarded"


def consumer_id(context: dict) -> Optional[str]:
    """Static identity of the consumer that sent a message, if any."""
    node = context.get("node") or {}
    if node.get("node_id"):
        return node["node_id"]
    if context.get("client_id"):
        return context["client_id"]
    session_id = (context.get("session") or {}).get("session_id")
    if session_id and session_id != LOCAL_SESSION_ID:
        return session_id
    return None


def user_id(context: dict) -> Optional[str]:
    """Authenticated user behind a message, if the context names one."""
    if context.get("user_id"):
        return context["user_id"]
    username = context.get("username")
    if username and username != LOCAL_USERNAME:
        return username
    for profile in context.get("user_profiles") or []:
        name = (profile.get("user") or {}).get("username")
        if name:
            return name
    return None


def is_forwarded(context: dict) -> bool:
    return bool(context.get(FORWARDED_MARKER))
