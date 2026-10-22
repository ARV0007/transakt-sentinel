"""
The investigator's line to transakt-mcp.

LangChain's MCP adapter connects to the MCP server, reads its tool menu and
turns each tool into a LangChain tool. This class adds the two things the
graph needs on top: call a tool by name with plain arguments, and get a
plain dict back (MCP returns results as text content blocks).
"""

import json
import logging

from langchain_mcp_adapters.client import MultiServerMCPClient

log = logging.getLogger("investigator.toolbox")


class ToolError(RuntimeError):
    pass


def _to_dict(result):
    """MCP tool results arrive as content blocks; ours carry one JSON text block."""
    if isinstance(result, tuple):          # (content, artifact) in some adapter versions
        result = result[0]
    if isinstance(result, dict):
        return result
    if isinstance(result, str):
        return json.loads(result)
    if isinstance(result, list):
        texts = [b.get("text", "") if isinstance(b, dict) else getattr(b, "text", str(b)) for b in result]
        return json.loads("".join(texts))
    raise ToolError(f"unexpected tool result type {type(result).__name__}")


class MCPToolbox:
    def __init__(self, url, token=None):
        self.url = url
        conn = {"url": url, "transport": "streamable_http"}
        if token:
            conn["headers"] = {"Authorization": f"Bearer {token}"}
        self.client = MultiServerMCPClient({"transakt": conn})
        self._tools = None

    async def tools(self):
        # Loaded lazily and retried on the next call if it fails: the MCP server
        # may still be starting when the investigator boots.
        if self._tools is None:
            self._tools = {t.name: t for t in await self.client.get_tools()}
            log.info("MCP tools available: %s", sorted(self._tools))
        return self._tools

    async def call(self, name, **args):
        try:
            tools = await self.tools()
        except Exception as e:
            raise ToolError(f"MCP server unreachable at {self.url}: {e}") from e
        if name not in tools:
            raise ToolError(f"MCP server has no tool named {name}")
        try:
            out = _to_dict(await tools[name].ainvoke(args))
        except Exception as e:
            raise ToolError(f"{name} failed: {e}") from e
        if isinstance(out, dict) and "error" in out and len(out) == 1:
            raise ToolError(out["error"])
        return out
