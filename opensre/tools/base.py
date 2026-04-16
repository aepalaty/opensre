"""Base tool interface for opensre integrations.

All tools must inherit from BaseTool and implement the required methods
as defined in .cursor/rules/tools.mdc.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolResult:
    """Encapsulates the result of a tool execution."""

    success: bool
    output: Any = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.success


class BaseTool(ABC):
    """Abstract base class for all opensre tools.

    Subclasses must implement:
        - my_tool_name (property)
        - is_available (method)
        - extract_params (method)
        - run (method)

    Example::

        class MyTool(BaseTool):
            @property
            def my_tool_name(self) -> str:
                return "my_tool"

            def is_available(self) -> bool:
                return True

            def extract_params(self, raw: dict) -> dict:
                return {"key": raw.get("key")}

            def run(self, params: dict) -> ToolResult:
                return ToolResult(success=True, output=params)
    """

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def my_tool_name(self) -> str:
        """Unique snake_case identifier for the tool."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def is_available(self) -> bool:
        """Return True when the tool's dependencies / credentials are ready."""

    @abstractmethod
    def extract_params(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Validate and normalise raw input into typed params.

        Args:
            raw: Arbitrary key/value pairs from the caller.

        Returns:
            A clean params dict consumed by :meth:`run`.

        Raises:
            ValueError: When required fields are missing or invalid.
        """

    @abstractmethod
    def run(self, params: dict[str, Any]) -> ToolResult:
        """Execute the tool with validated params.

        Args:
            params: Output of :meth:`extract_params`.

        Returns:
            A :class:`ToolResult` describing the outcome.
        """

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def execute(self, raw: dict[str, Any]) -> ToolResult:
        """High-level entry point: validate then run.

        Wraps :meth:`extract_params` + :meth:`run` and catches unexpected
        exceptions so callers always receive a :class:`ToolResult`.
        """
        if not self.is_available():
            return ToolResult(
                success=False,
                error=f"Tool '{self.my_tool_name}' is not available in this environment.",
            )
        try:
            params = self.extract_params(raw)
            return self.run(params)
        except ValueError as exc:
            return ToolResult(success=False, error=f"Invalid params: {exc}")
        except Exception as exc:  # noqa: BLE001
            return ToolResult(success=False, error=f"Unexpected error: {exc}")

    def __repr__(self) -> str:
        available = self.is_available()
        return f"<{self.__class__.__name__} name={self.my_tool_name!r} available={available}>"
