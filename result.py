from __future__ import annotations
from typing import Generic, TypeVar, Any, Callable, Optional

# 定义泛型类型变量
T = TypeVar('T')  # 成功类型
E = TypeVar('E')  # 错误类型


class Ok(Generic[T]):
    """表示操作成功的结果，类似 Rust 的 Ok(T)"""
    def __init__(self, value: T):
        self.value = value

    def is_ok(self) -> bool:
        return True

    def is_err(self) -> bool:
        return False

    def unwrap(self) -> T:
        """获取成功值，如果是错误则抛出异常"""
        return self.value

    def unwrap_or(self, default: T) -> T:
        """获取成功值，失败时返回默认值（这里总是返回成功值）"""
        return self.value

    def map(self, func: Callable[[T], Any]) -> Ok[Any]:
        """对成功值应用函数，返回新的 Ok"""
        return Ok(func(self.value))

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


class Err(Generic[E]):
    """表示操作失败的结果，类似 Rust 的 Err(E)"""
    def __init__(self, error: E):
        self.error = error

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True

    def unwrap(self) -> Never:
        """获取成功值，如果是错误则抛出异常"""
        raise ValueError(f"Called unwrap() on Err: {self.error}")

    def unwrap_or(self, default: T) -> T:
        """获取成功值，失败时返回默认值"""
        return default

    def map(self, func: Callable[[Any], Any]) -> Err[E]:
        """对成功值应用函数（错误时不执行，直接返回自身）"""
        return self

    def __repr__(self) -> str:
        return f"Err({self.error!r})"


# 定义 Result 类型，为 Ok 和 Err 的联合类型
Result = Ok[T] | Err[E]