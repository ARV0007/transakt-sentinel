import pytest


@pytest.fixture(autouse=True)
def fresh_mcp_session_manager():
    # The MCP SDK lets a session manager start once per process. Production
    # starts exactly one server; tests start several, so give each a new one.
    from app.server import mcp
    mcp._session_manager = None
    yield
